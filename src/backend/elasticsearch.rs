use std::{
    error::Error,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use reqwest::{Client, RequestBuilder, StatusCode, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{sync::watch, task::JoinSet};
use tracing::{debug, error, info, warn};

use crate::{action::dispatch, status::RunnerHealth, task_message::parse_action};

type BoxError = Box<dyn Error + Send + Sync>;

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

pub struct Config {
    url: Url,
    index: String,
    concurrency: usize,
    lease_secs: u64,
    max_attempts: u64,
    retry_delay_secs: u64,
    auth: Auth,
}

enum Auth {
    None,
    ApiKey(String),
    Basic { username: String, password: String },
}

impl Config {
    pub fn from_env() -> Result<Self, BoxError> {
        let url = std::env::var("ES_URL")?;
        let url = Url::parse(&url)?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err("ES_URL must be an absolute http(s) URL".into());
        }

        let index = std::env::var("ES_TASK_INDEX").unwrap_or_else(|_| "tasks".to_owned());
        if index.trim().is_empty() {
            return Err("ES_TASK_INDEX must not be empty".into());
        }
        let concurrency = std::env::var("WORKER_CONCURRENCY")
            .unwrap_or_else(|_| "4".to_owned())
            .parse::<usize>()?;
        if concurrency == 0 {
            return Err("WORKER_CONCURRENCY must be greater than zero".into());
        }

        let lease_secs = env_u64("ES_TASK_LEASE_SECS", 900)?;
        let max_attempts = env_u64("ES_MAX_ATTEMPTS", 5)?;
        let retry_delay_secs = env_u64("ES_RETRY_DELAY_SECS", 5)?;
        let helm_timeout_secs = env_u64("HELM_TIMEOUT_SECS", 600)?;
        if lease_secs <= helm_timeout_secs {
            return Err("ES_TASK_LEASE_SECS must be greater than HELM_TIMEOUT_SECS".into());
        }
        if max_attempts == 0 {
            return Err("ES_MAX_ATTEMPTS must be greater than zero".into());
        }
        if retry_delay_secs == 0 {
            return Err("ES_RETRY_DELAY_SECS must be greater than zero".into());
        }

        let api_key = std::env::var("ES_API_KEY").ok();
        let username = std::env::var("ES_USERNAME").ok();
        let password = std::env::var("ES_PASSWORD").ok();
        let auth = match (api_key, username, password) {
            (Some(key), None, None) if !key.is_empty() => Auth::ApiKey(key),
            (None, Some(username), Some(password)) if !username.is_empty() => {
                Auth::Basic { username, password }
            }
            (None, None, None) => Auth::None,
            _ => {
                return Err(
                    "configure either ES_API_KEY or both ES_USERNAME and ES_PASSWORD".into(),
                );
            }
        };

        Ok(Self {
            url,
            index,
            concurrency,
            lease_secs,
            max_attempts,
            retry_delay_secs,
            auth,
        })
    }
}

fn env_u64(name: &str, default: u64) -> Result<u64, BoxError> {
    Ok(std::env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse()?)
}

#[derive(Clone)]
struct ClientState {
    client: Client,
    base_url: Url,
    index: String,
    auth: Arc<Auth>,
}

impl ClientState {
    fn new(config: &Config) -> Result<Self, BoxError> {
        Ok(Self {
            client: Client::builder().build()?,
            base_url: config.url.clone(),
            index: config.index.clone(),
            auth: Arc::new(match &config.auth {
                Auth::None => Auth::None,
                Auth::ApiKey(key) => Auth::ApiKey(key.clone()),
                Auth::Basic { username, password } => Auth::Basic {
                    username: username.clone(),
                    password: password.clone(),
                },
            }),
        })
    }

    fn endpoint(&self, segments: &[&str]) -> Result<Url, BoxError> {
        let mut url = self.base_url.clone();
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| "ES_URL cannot be used as a hierarchical URL")?;
            path.pop_if_empty();
            for segment in segments {
                path.push(segment);
            }
        }
        Ok(url)
    }

    fn authenticated(&self, request: RequestBuilder) -> RequestBuilder {
        match self.auth.as_ref() {
            Auth::None => request,
            Auth::ApiKey(key) => request.header("Authorization", format!("ApiKey {key}")),
            Auth::Basic { username, password } => request.basic_auth(username, Some(password)),
        }
    }

    async fn search(&self, limit: usize) -> Result<Vec<Hit>, BoxError> {
        let url = self.endpoint(&[&self.index, "_search"])?;
        let now = now_ms();
        let response = self
            .authenticated(self.client.post(url))
            .query(&[("seq_no_primary_term", "true")])
            .json(&json!({
                "size": limit,
                "query": {
                    "bool": {
                        "should": [
                            {"bool": {"filter": [{"match": {"status": "pending"}}], "must_not": [{"exists": {"field": "next_attempt_at"}}]}},
                            {"bool": {"filter": [{"match": {"status": "pending"}}, {"range": {"next_attempt_at": {"lte": now}}}]}},
                            {"bool": {"filter": [{"match": {"status": "processing"}}, {"range": {"lease_expires_at": {"lte": now}}}]}}
                        ],
                        "minimum_should_match": 1
                    }
                }
            }))
            .send()
            .await?;
        let response = checked_response(response).await?;
        Ok(response.json::<SearchResponse>().await?.hits.hits)
    }

    async fn claim(
        &self,
        hit: &Hit,
        worker_id: &str,
        lease_secs: u64,
    ) -> Result<Option<Claim>, BoxError> {
        let now = now_ms();
        let lease_expires_at = now.saturating_add(lease_secs.saturating_mul(1000));
        let lease_token = format!("{worker_id}:{}:{}", hit.seq_no, now);
        let mut source = hit.source.clone();
        let Some(document) = source.as_object_mut() else {
            return Err(format!("task document '{}' must be a JSON object", hit.id).into());
        };
        let attempts = document
            .get("attempts")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .saturating_add(1);
        document.insert("status".to_owned(), json!("processing"));
        document.insert("attempts".to_owned(), json!(attempts));
        document.insert("locked_by".to_owned(), json!(worker_id));
        document.insert("lease_token".to_owned(), json!(lease_token));
        document.insert("lease_expires_at".to_owned(), json!(lease_expires_at));
        document.remove("next_attempt_at");
        let token = document["lease_token"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let source = source.clone();

        let mut url = self.endpoint(&[&self.index, "_doc", &hit.id])?;
        url.query_pairs_mut()
            .append_pair("if_seq_no", &hit.seq_no.to_string())
            .append_pair("if_primary_term", &hit.primary_term.to_string());
        let response = self
            .authenticated(self.client.put(url))
            .json(&source)
            .send()
            .await?;
        if response.status() == StatusCode::CONFLICT {
            return Ok(None);
        }
        checked_response(response).await?;
        Ok(Some(Claim {
            id: hit.id.clone(),
            token,
            attempts,
            source,
        }))
    }

    async fn finish(
        &self,
        claim: &Claim,
        worker_id: &str,
        status: &str,
        error_message: Option<&str>,
        retry_at: Option<u64>,
    ) -> Result<(), BoxError> {
        let script = "if (ctx._source.lease_token != params.token || ctx._source.locked_by != params.worker) { ctx.op = 'noop'; } else { ctx._source.status = params.status; ctx._source.updated_at = params.now; ctx._source.remove('locked_by'); ctx._source.remove('lease_token'); ctx._source.remove('lease_expires_at'); if (params.retry_at == null) { ctx._source.remove('next_attempt_at'); } else { ctx._source.next_attempt_at = params.retry_at; } if (params.error == null) { ctx._source.remove('last_error'); } else { ctx._source.last_error = params.error; } }";
        let url = self.endpoint(&[&self.index, "_update", &claim.id])?;
        let response = self
            .authenticated(self.client.post(url))
            .json(&json!({
                "script": {
                    "lang": "painless",
                    "source": script,
                    "params": {
                        "token": claim.token,
                        "worker": worker_id,
                        "status": status,
                        "now": now_ms(),
                        "retry_at": retry_at,
                        "error": error_message
                    }
                }
            }))
            .send()
            .await?;
        let body: Value = checked_response(response).await?.json().await?;
        if body.get("result").and_then(Value::as_str) == Some("noop") {
            warn!(task_id = %claim.id, "task lease was lost before task status could be updated");
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct SearchResponse {
    hits: SearchHits,
}

#[derive(Deserialize)]
struct SearchHits {
    hits: Vec<Hit>,
}

#[derive(Deserialize)]
struct Hit {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_seq_no")]
    seq_no: u64,
    #[serde(rename = "_primary_term")]
    primary_term: u64,
    #[serde(rename = "_source")]
    source: Value,
}

struct Claim {
    id: String,
    token: String,
    attempts: u64,
    source: Value,
}

async fn checked_response(response: reqwest::Response) -> Result<reqwest::Response, BoxError> {
    if response.status().is_success() {
        Ok(response)
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(format!("Elasticsearch returned {status}: {body}").into())
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn worker_id() -> String {
    format!("task-worker-{}-{}", std::process::id(), now_ms())
}

pub async fn run(
    config: Config,
    health: Arc<RunnerHealth>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), BoxError> {
    let client = ClientState::new(&config)?;
    let worker_id = worker_id();
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        health.set_connecting();
        info!(index = %config.index, "connecting to Elasticsearch task index");
        match consume(&config, &client, &worker_id, &health, &mut shutdown).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                health.set_disconnected();
                error!(index = %config.index, error = %error, "Elasticsearch consumer failed; retrying in 5s");
                tokio::select! {
                    _ = tokio::time::sleep(RECONNECT_DELAY) => {}
                    _ = shutdown.changed() => return Ok(()),
                }
            }
        }
    }
}

async fn consume(
    config: &Config,
    client: &ClientState,
    worker_id: &str,
    health: &Arc<RunnerHealth>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), BoxError> {
    let mut tasks = JoinSet::new();
    let mut backend_ready = false;
    info!(index = %config.index, concurrency = config.concurrency, "Elasticsearch task poller started");
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

        let free_slots = config.concurrency - tasks.len();
        let hits = client.search(free_slots).await?;
        if !backend_ready {
            health.set_waiting();
            backend_ready = true;
            info!(index = %config.index, "Elasticsearch connection is ready");
        }
        let mut claimed_any = false;
        for hit in hits {
            let Some(claim) = client.claim(&hit, worker_id, config.lease_secs).await? else {
                debug!(task_id = %hit.id, "task claim lost to another worker");
                continue;
            };
            claimed_any = true;
            if claim.attempts > config.max_attempts {
                warn!(task_id = %claim.id, attempts = claim.attempts, max_attempts = config.max_attempts, "task exceeded its attempt limit while its previous lease was active; marking it dead");
                client
                    .finish(
                        &claim,
                        worker_id,
                        "dead",
                        Some("maximum task attempts exceeded"),
                        None,
                    )
                    .await?;
                continue;
            }
            let action = match parse_action(&serde_json::to_vec(&claim.source)?) {
                Ok(action) => action,
                Err(parse_error) => {
                    warn!(task_id = %claim.id, error = %parse_error, "invalid Elasticsearch task; marking it dead");
                    client
                        .finish(&claim, worker_id, "dead", Some(&parse_error), None)
                        .await?;
                    continue;
                }
            };
            let action_name = match &action {
                crate::action::Action::Deploy(_) => "deploy",
                crate::action::Action::Undeploy(_) => "undeploy",
            };
            let task_client = client.clone();
            let task_worker_id = worker_id.to_owned();
            let task_health = Arc::clone(health);
            let max_attempts = config.max_attempts;
            let base_retry = config.retry_delay_secs;
            tasks.spawn(async move {
                let _active_task = ActiveTask::new(task_health);
                info!(task_id = %claim.id, action = action_name, attempt = claim.attempts, "starting Elasticsearch task");
                match dispatch(action).await {
                    Ok(()) => {
                        task_client.finish(&claim, &task_worker_id, "completed", None, None).await?;
                        info!(task_id = %claim.id, action = action_name, "task completed");
                    }
                    Err(task_error) => {
                        let final_attempt = claim.attempts >= max_attempts;
                        let status = if final_attempt { "dead" } else { "pending" };
                        let retry_at = if final_attempt {
                            None
                        } else {
                            Some(now_ms().saturating_add(retry_delay(base_retry, claim.attempts)))
                        };
                        error!(task_id = %claim.id, action = action_name, attempt = claim.attempts, final_attempt, error = %task_error, "task execution failed");
                        task_client
                            .finish(&claim, &task_worker_id, status, Some(&task_error.to_string()), retry_at)
                            .await?;
                    }
                }
                Ok::<(), BoxError>(())
            });
            if tasks.len() >= config.concurrency {
                break;
            }
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

#[cfg(test)]
mod tests {
    use super::retry_delay;

    #[test]
    fn retry_delay_doubles_and_caps_at_five_minutes() {
        assert_eq!(retry_delay(5, 1), 5_000);
        assert_eq!(retry_delay(5, 2), 10_000);
        assert_eq!(retry_delay(5, 20), 300_000);
    }
}
