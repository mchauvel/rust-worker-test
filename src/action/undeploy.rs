use std::{error::Error, process::Stdio, time::Duration};

use tokio::{process::Command, time::timeout};
use tracing::{error, info, warn};

use crate::action::UndeployPayload;

pub(super) async fn run(payload: UndeployPayload) -> Result<(), Box<dyn Error + Send + Sync>> {
    payload.validate()?;

    let helm_bin = std::env::var("HELM_BIN").unwrap_or_else(|_| "helm".to_owned());
    let timeout_secs = std::env::var("HELM_TIMEOUT_SECS")
        .unwrap_or_else(|_| "600".to_owned())
        .parse::<u64>()
        .unwrap_or(600);

    info!(
        release_name = %payload.release_name,
        namespace = %payload.namespace,
        helm_bin = %helm_bin,
        timeout_secs = timeout_secs,
        "starting helm uninstall"
    );

    let mut command = Command::new(&helm_bin);
    command
        .arg("uninstall")
        .arg(&payload.release_name)
        .arg("--namespace")
        .arg(&payload.namespace)
        .arg("--wait")
        .arg("--timeout")
        .arg(format!("{timeout_secs}s"))
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let mut child = command.spawn()?;
    let result = timeout(Duration::from_secs(timeout_secs), child.wait()).await;

    match result {
        Ok(status) => {
            let status = status?;
            if status.success() {
                info!(
                    release_name = %payload.release_name,
                    namespace = %payload.namespace,
                    "helm uninstall completed successfully"
                );
                Ok(())
            } else {
                error!(
                    release_name = %payload.release_name,
                    namespace = %payload.namespace,
                    exit_status = ?status,
                    "helm uninstall failed"
                );
                Err(format!("helm uninstall failed with status {status}").into())
            }
        }
        Err(_) => {
            warn!(release_name = %payload.release_name, helm_bin = %helm_bin, timeout_secs = timeout_secs, "helm uninstall timed out; killing child process");
            let _ = child.kill().await;
            Err(format!("helm uninstall timed out after {timeout_secs}s").into())
        }
    }
}
