use std::{
    error::Error,
    ffi::OsStr,
    fs::OpenOptions,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use serde_json::{Value, json};
use tokio::{fs, sync::watch, task::JoinSet};
use tracing::{debug, error, info, warn};

use crate::{action::dispatch, status::RunnerHealth, task_message::parse_action};

type BoxError = Box<dyn Error + Send + Sync>;

const POLL_INTERVAL: Duration = Duration::from_secs(1);

pub struct Config {
    root: PathBuf,
    concurrency: usize,
    lease_secs: u64,
    max_attempts: u64,
    retry_delay_secs: u64,
}

impl Config {
    pub fn from_env() -> Result<Self, BoxError> {
        let root = std::env::var("FILE_TASK_DIR").unwrap_or_else(|_| ".local/tasks".to_owned());
        let concurrency = std::env::var("WORKER_CONCURRENCY")
            .unwrap_or_else(|_| "4".to_owned())
            .parse::<usize>()?;
        let lease_secs = env_u64("FILE_TASK_LEASE_SECS", 900)?;
        let max_attempts = env_u64("FILE_MAX_ATTEMPTS", 5)?;
        let retry_delay_secs = env_u64("FILE_RETRY_DELAY_SECS", 5)?;
        let helm_timeout_secs = env_u64("HELM_TIMEOUT_SECS", 600)?;

        if root.trim().is_empty() {
            return Err("FILE_TASK_DIR must not be empty".into());
        }
        if concurrency == 0 {
            return Err("WORKER_CONCURRENCY must be greater than zero".into());
        }
        if lease_secs <= helm_timeout_secs {
            return Err("FILE_TASK_LEASE_SECS must be greater than HELM_TIMEOUT_SECS".into());
        }
        if max_attempts == 0 {
            return Err("FILE_MAX_ATTEMPTS must be greater than zero".into());
        }
        if retry_delay_secs == 0 {
            return Err("FILE_RETRY_DELAY_SECS must be greater than zero".into());
        }

        Ok(Self {
            root: PathBuf::from(root),
            concurrency,
            lease_secs,
            max_attempts,
            retry_delay_secs,
        })
    }
}

fn env_u64(name: &str, default: u64) -> Result<u64, BoxError> {
    Ok(std::env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse()?)
}

struct Directories {
    claim_lock: PathBuf,
    pending: PathBuf,
    processing: PathBuf,
    completed: PathBuf,
    dead: PathBuf,
}

impl Directories {
    fn new(root: &Path) -> Self {
        Self {
            claim_lock: root.join(".claim.lock"),
            pending: root.join("pending"),
            processing: root.join("processing"),
            completed: root.join("completed"),
            dead: root.join("dead"),
        }
    }

    async fn create(&self, worker_id: &str) -> Result<(), BoxError> {
        for directory in [
            &self.pending,
            &self.processing,
            &self.completed,
            &self.dead,
            &self.processing.join(worker_id),
        ] {
            fs::create_dir_all(directory).await?;
        }
        Ok(())
    }
}

struct Claim {
    path: PathBuf,
    filename: String,
    document: Value,
    attempts: u64,
}

struct ActiveTask(Arc<RunnerHealth>);

impl ActiveTask {
    fn new(health: Arc<RunnerHealth>) -> Self {
        health.task_started();
        Self(health)
    }
}

impl Drop for ActiveTask {
    fn drop(&mut self) {
        self.0.task_finished();
    }
}

pub async fn run(
    config: Config,
    health: Arc<RunnerHealth>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), BoxError> {
    let worker_id = format!("{}-{}", std::process::id(), now_ms());
    let directories = Directories::new(&config.root);
    directories.create(&worker_id).await?;
    health.set_waiting();
    info!(
        task_dir = %config.root.display(),
        concurrency = config.concurrency,
        "local file task backend started"
    );

    let mut tasks = JoinSet::new();
    loop {
        if *shutdown.borrow() {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            return Ok(());
        }
        if tasks.len() >= config.concurrency {
            tokio::select! {
                _ = shutdown.changed() => continue,
                Some(result) = tasks.join_next() => result??,
            }
            continue;
        }

        recover_expired(&directories, &worker_id, config.max_attempts).await?;
        let pending_files = list_json_files(&directories.pending).await?;
        let mut claimed_any = false;

        for pending_path in pending_files {
            if tasks.len() >= config.concurrency {
                break;
            }
            if !is_due(&pending_path).await? {
                continue;
            }

            let filename = pending_path
                .file_name()
                .unwrap_or_else(|| OsStr::new("task.json"))
                .to_string_lossy()
                .into_owned();
            let processing_path = directories.processing.join(&worker_id).join(&filename);
            if !claim_file(&pending_path, &processing_path, &directories.claim_lock).await? {
                debug!(task_id = %filename, "another local worker claimed task first");
                continue;
            }
            claimed_any = true;

            let mut document = match fs::read(&processing_path).await {
                Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                    Ok(document) => document,
                    Err(parse_error) => {
                        warn!(task_id = %filename, error = %parse_error, "invalid JSON task; moving to dead folder");
                        fs::rename(&processing_path, directories.dead.join(&filename)).await?;
                        continue;
                    }
                },
                Err(error) => return Err(error.into()),
            };

            let Some(fields) = document.as_object_mut() else {
                warn!(task_id = %filename, "task JSON must be an object; moving to dead folder");
                fs::rename(&processing_path, directories.dead.join(&filename)).await?;
                continue;
            };
            let attempts = fields
                .get("attempts")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .saturating_add(1);
            fields.insert("status".to_owned(), json!("processing"));
            fields.insert("attempts".to_owned(), json!(attempts));
            fields.insert("locked_by".to_owned(), json!(worker_id));
            fields.insert(
                "lease_expires_at".to_owned(),
                json!(now_ms().saturating_add(config.lease_secs.saturating_mul(1000))),
            );
            fields.remove("next_attempt_at");
            if attempts > config.max_attempts {
                fields.insert("status".to_owned(), json!("dead"));
                fields.insert(
                    "last_error".to_owned(),
                    json!("maximum task attempts exceeded"),
                );
                fs::write(&processing_path, serde_json::to_vec_pretty(&document)?).await?;
                fs::rename(&processing_path, directories.dead.join(&filename)).await?;
                continue;
            }
            fs::write(&processing_path, serde_json::to_vec_pretty(&document)?).await?;

            let action = match parse_action(&serde_json::to_vec(&document)?) {
                Ok(action) => action,
                Err(parse_error) => {
                    warn!(task_id = %filename, error = %parse_error, "invalid task payload; moving to dead folder");
                    mark_status(
                        &processing_path,
                        &mut document,
                        "dead",
                        Some(&parse_error),
                        None,
                    )
                    .await?;
                    fs::rename(&processing_path, directories.dead.join(&filename)).await?;
                    continue;
                }
            };

            let action_name = match &action {
                crate::action::Action::Deploy(_) => "deploy",
                crate::action::Action::Undeploy(_) => "undeploy",
            };
            let claim = Claim {
                path: processing_path,
                filename,
                document,
                attempts,
            };
            let task_directories = Directories {
                claim_lock: directories.claim_lock.clone(),
                pending: directories.pending.clone(),
                processing: directories.processing.clone(),
                completed: directories.completed.clone(),
                dead: directories.dead.clone(),
            };
            let task_health = Arc::clone(&health);
            let max_attempts = config.max_attempts;
            let retry_delay_secs = config.retry_delay_secs;
            tasks.spawn(async move {
                let _active_task = ActiveTask::new(task_health);
                info!(task_id = %claim.filename, action = action_name, attempt = claim.attempts, "starting file-backed task");
                match dispatch(action).await {
                    Ok(()) => {
                        mark_status(&claim.path, &mut claim.document.clone(), "completed", None, None).await?;
                        fs::rename(&claim.path, task_directories.completed.join(&claim.filename)).await?;
                        info!(task_id = %claim.filename, action = action_name, "task completed");
                    }
                    Err(task_error) => {
                        let final_attempt = claim.attempts >= max_attempts;
                        if final_attempt {
                            mark_status(
                                &claim.path,
                                &mut claim.document.clone(),
                                "dead",
                                Some(&task_error.to_string()),
                                None,
                            )
                            .await?;
                            fs::rename(&claim.path, task_directories.dead.join(&claim.filename)).await?;
                        } else {
                            let retry_at = now_ms().saturating_add(retry_delay(retry_delay_secs, claim.attempts));
                            mark_status(
                                &claim.path,
                                &mut claim.document.clone(),
                                "pending",
                                Some(&task_error.to_string()),
                                Some(retry_at),
                            )
                            .await?;
                            fs::rename(&claim.path, task_directories.pending.join(&claim.filename)).await?;
                        }
                        error!(task_id = %claim.filename, action = action_name, attempt = claim.attempts, final_attempt, error = %task_error, "file-backed task failed");
                    }
                }
                Ok::<(), BoxError>(())
            });
        }

        if tasks.is_empty() || !claimed_any {
            tokio::select! {
                _ = tokio::time::sleep(POLL_INTERVAL) => {},
                _ = shutdown.changed() => {},
                Some(result) = tasks.join_next(), if !tasks.is_empty() => result??,
            }
        }
    }
}

async fn list_json_files(directory: &Path) -> Result<Vec<PathBuf>, BoxError> {
    let mut entries = fs::read_dir(directory).await?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_file() && path.extension() == Some(OsStr::new("json")) {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

async fn is_due(path: &Path) -> Result<bool, BoxError> {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let Ok(document) = serde_json::from_slice::<Value>(&bytes) else {
        return Ok(true);
    };
    Ok(document
        .get("next_attempt_at")
        .and_then(Value::as_u64)
        .is_none_or(|next_attempt| next_attempt <= now_ms()))
}

async fn claim_file(source: &Path, destination: &Path, lock_path: &Path) -> Result<bool, BoxError> {
    let source = source.to_owned();
    let destination = destination.to_owned();
    let lock_path = lock_path.to_owned();
    tokio::task::spawn_blocking(move || {
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        lock_file.lock_exclusive()?;
        let result = std::fs::rename(source, destination);
        FileExt::unlock(&lock_file)?;

        match result {
            Ok(()) => Ok(true),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::AlreadyExists
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    })
    .await?
    .map_err(Into::into)
}

async fn recover_expired(
    directories: &Directories,
    worker_id: &str,
    max_attempts: u64,
) -> Result<(), BoxError> {
    let mut owners = fs::read_dir(&directories.processing).await?;
    while let Some(owner) = owners.next_entry().await? {
        if !owner.file_type().await?.is_dir() || owner.file_name() == worker_id {
            continue;
        }
        let mut tasks = fs::read_dir(owner.path()).await?;
        while let Some(entry) = tasks.next_entry().await? {
            let source = entry.path();
            if source.extension() != Some(OsStr::new("json")) {
                continue;
            }
            let bytes = match fs::read(&source).await {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let mut document = match serde_json::from_slice::<Value>(&bytes) {
                Ok(document) => document,
                Err(_) => {
                    let filename = source.file_name().unwrap_or_default();
                    if claim_file(
                        &source,
                        &directories.dead.join(filename),
                        &directories.claim_lock,
                    )
                    .await?
                    {
                        warn!(task_id = %filename.to_string_lossy(), "corrupt processing task moved to dead folder");
                    }
                    continue;
                }
            };
            let lease_expires = document
                .get("lease_expires_at")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            if lease_expires > now_ms() {
                continue;
            }

            let original_filename = source.file_name().unwrap_or_default().to_owned();
            let temporary_claim = directories.processing.join(worker_id).join(format!(
                "recovered-{}-{}",
                owner.file_name().to_string_lossy(),
                original_filename.to_string_lossy()
            ));
            if !claim_file(&source, &temporary_claim, &directories.claim_lock).await? {
                continue;
            }

            let attempts = document
                .get("attempts")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if attempts >= max_attempts {
                mark_status(
                    &temporary_claim,
                    &mut document,
                    "dead",
                    Some("maximum task attempts exceeded after lease expiration"),
                    None,
                )
                .await?;
                fs::rename(&temporary_claim, directories.dead.join(&original_filename)).await?;
                warn!(task_id = %original_filename.to_string_lossy(), attempts, "expired task lease exhausted attempts; task moved to dead folder");
            } else {
                mark_status(&temporary_claim, &mut document, "pending", None, None).await?;
                fs::rename(
                    &temporary_claim,
                    directories.pending.join(&original_filename),
                )
                .await?;
                warn!(task_id = %original_filename.to_string_lossy(), "expired task lease reclaimed");
            }
        }
    }
    Ok(())
}

async fn mark_status(
    path: &Path,
    document: &mut Value,
    status: &str,
    error_message: Option<&str>,
    next_attempt_at: Option<u64>,
) -> Result<(), BoxError> {
    let Some(fields) = document.as_object_mut() else {
        return Err("task document must be a JSON object".into());
    };
    fields.insert("status".to_owned(), json!(status));
    fields.insert("updated_at".to_owned(), json!(now_ms()));
    fields.remove("locked_by");
    fields.remove("lease_expires_at");
    match error_message {
        Some(message) => {
            fields.insert("last_error".to_owned(), json!(message));
        }
        None => {
            fields.remove("last_error");
        }
    }
    match next_attempt_at {
        Some(timestamp) => {
            fields.insert("next_attempt_at".to_owned(), json!(timestamp));
        }
        None => {
            fields.remove("next_attempt_at");
        }
    }
    fs::write(path, serde_json::to_vec_pretty(document)?).await?;
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn retry_delay(base_secs: u64, attempts: u64) -> u64 {
    base_secs
        .saturating_mul(
            1_u64
                .checked_shl(attempts.saturating_sub(1).min(16) as u32)
                .unwrap_or(u64::MAX),
        )
        .min(300)
        .saturating_mul(1000)
}

#[cfg(test)]
mod tests {
    use std::{
        time::{SystemTime, UNIX_EPOCH},
    };

    use tokio::fs;

    use super::claim_file;

    #[tokio::test]
    async fn only_one_worker_can_claim_a_pending_file() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("task-worker-file-test-{unique}"));
        let pending = root.join("pending");
        let first_processing = root.join("processing-first");
        let second_processing = root.join("processing-second");
        fs::create_dir_all(&pending).await.unwrap();
        fs::create_dir_all(&first_processing).await.unwrap();
        fs::create_dir_all(&second_processing).await.unwrap();

        let source = pending.join("task-1.json");
        let first_claim = first_processing.join("task-1.json");
        let second_claim = second_processing.join("task-1.json");
        let lock = root.join(".claim.lock");
        fs::write(&source, br#"{"action":"undeploy","release_name":"demo"}"#)
            .await
            .unwrap();

        let (first_won, second_won) = tokio::join!(
            claim_file(&source, &first_claim, &lock),
            claim_file(&source, &second_claim, &lock)
        );
        let first_won = first_won.unwrap();
        let second_won = second_won.unwrap();

        assert_ne!(first_won, second_won);
        assert_eq!(fs::try_exists(&first_claim).await.unwrap(), first_won);
        assert_eq!(fs::try_exists(&second_claim).await.unwrap(), second_won);
        assert!(!fs::try_exists(&source).await.unwrap());

        fs::remove_dir_all(root).await.unwrap();
    }
}
