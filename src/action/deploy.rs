use std::{error::Error, process::Stdio, time::Duration};

use tokio::{process::Command, time::timeout};
use tracing::{error, info, warn};

use crate::action::DeployPayload;

fn build_helm_upgrade_args(payload: &DeployPayload, timeout_secs: u64) -> Vec<String> {
    let mut args = vec![
        "upgrade".to_string(),
        "--install".to_string(),
        payload.release_name.clone(),
        payload.chart.clone(),
        "--namespace".to_string(),
        payload.namespace.clone(),
        "--timeout".to_string(),
        format!("{timeout_secs}s"),
        "--create-namespace".to_string(),
        "--wait".to_string(),
        "--atomic".to_string(),
    ];

    for value in &payload.values {
        args.push("--values".to_string());
        args.push(value.clone());
    }
    for set_value in &payload.set {
        args.push("--set".to_string());
        args.push(set_value.clone());
    }

    args
}

pub(super) async fn run(payload: DeployPayload) -> Result<(), Box<dyn Error + Send + Sync>> {
    payload.validate()?;

    let helm_bin = std::env::var("HELM_BIN").unwrap_or_else(|_| "helm".to_owned());
    let timeout_secs = std::env::var("HELM_TIMEOUT_SECS")
        .unwrap_or_else(|_| "600".to_owned())
        .parse::<u64>()
        .unwrap_or(600);

    info!(
        release_name = %payload.release_name,
        chart = %payload.chart,
        namespace = %payload.namespace,
        values_count = payload.values.len(),
        set_count = payload.set.len(),
        helm_bin = %helm_bin,
        timeout_secs = timeout_secs,
        "starting helm upgrade --install"
    );

    let args = build_helm_upgrade_args(&payload, timeout_secs);
    let mut command = Command::new(&helm_bin);
    command
        .args(&args)
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
                    chart = %payload.chart,
                    namespace = %payload.namespace,
                    "helm upgrade --install completed successfully"
                );
                Ok(())
            } else {
                error!(
                    release_name = %payload.release_name,
                    chart = %payload.chart,
                    namespace = %payload.namespace,
                    exit_status = ?status,
                    "helm upgrade --install failed"
                );
                Err(format!("helm upgrade --install failed with status {status}").into())
            }
        }
        Err(_) => {
            warn!(release_name = %payload.release_name, helm_bin = %helm_bin, timeout_secs = timeout_secs, "helm upgrade --install timed out; killing child process");
            let _ = child.kill().await;
            Err(format!("helm upgrade --install timed out after {timeout_secs}s").into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::build_helm_upgrade_args;
    use crate::action::DeployPayload;

    #[test]
    fn deploy_uses_upgrade_install_for_existing_releases() {
        let payload = DeployPayload {
            release_name: "demo".to_string(),
            chart: "./chart".to_string(),
            namespace: "prod".to_string(),
            values: vec!["values.yaml".to_string()],
            set: vec!["image.tag=1.2.3".to_string()],
        };

        let args = build_helm_upgrade_args(&payload, 120);

        assert!(args.iter().any(|arg| arg == "upgrade"));
        assert!(args.iter().any(|arg| arg == "--install"));
        assert!(args.iter().any(|arg| arg == "--atomic"));
        assert!(args.iter().any(|arg| arg == "--wait"));
        assert!(args.iter().any(|arg| arg == "--values"));
        assert!(args.iter().any(|arg| arg == "--set"));
    }
}
