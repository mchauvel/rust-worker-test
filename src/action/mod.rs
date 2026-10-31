use std::error::Error;

use serde::Deserialize;

mod deploy;
mod undeploy;

type BoxError = Box<dyn Error + Send + Sync>;

fn default_namespace() -> String {
    "default".to_string()
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct DeployPayload {
    pub release_name: String,
    pub chart: String,
    #[serde(default = "default_namespace")]
    pub namespace: String,
    #[serde(default)]
    pub values: Vec<String>,
    #[serde(default)]
    pub set: Vec<String>,
}

impl DeployPayload {
    pub fn validate(&self) -> Result<(), BoxError> {
        if self.release_name.trim().is_empty() {
            return Err("release_name must not be empty".into());
        }
        if self.chart.trim().is_empty() {
            return Err("chart must not be empty".into());
        }
        if self.namespace.trim().is_empty() {
            return Err("namespace must not be empty".into());
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct UndeployPayload {
    pub release_name: String,
    #[serde(default = "default_namespace")]
    pub namespace: String,
}

impl UndeployPayload {
    pub fn validate(&self) -> Result<(), BoxError> {
        if self.release_name.trim().is_empty() {
            return Err("release_name must not be empty".into());
        }
        if self.namespace.trim().is_empty() {
            return Err("namespace must not be empty".into());
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Deploy(DeployPayload),
    Undeploy(UndeployPayload),
}

pub async fn dispatch(action: Action) -> Result<(), BoxError> {
    match action {
        Action::Deploy(payload) => deploy::run(payload).await,
        Action::Undeploy(payload) => undeploy::run(payload).await,
    }
}

#[cfg(test)]
mod tests {
    use super::{Action, DeployPayload, UndeployPayload};

    #[test]
    fn deploy_payload_requires_non_empty_release_name() {
        let payload = DeployPayload {
            release_name: String::new(),
            chart: "./chart".to_string(),
            namespace: "default".to_string(),
            values: vec![],
            set: vec![],
        };

        assert!(payload.validate().is_err());
    }

    #[test]
    fn deploy_payload_accepts_valid_values() {
        let payload = DeployPayload {
            release_name: "demo".to_string(),
            chart: "./chart".to_string(),
            namespace: "prod".to_string(),
            values: vec!["values.yaml".to_string()],
            set: vec!["image.tag=1.2.3".to_string()],
        };

        assert!(payload.validate().is_ok());
    }

    #[test]
    fn undeploy_payload_requires_non_empty_release_name() {
        let payload = UndeployPayload {
            release_name: String::new(),
            namespace: "default".to_string(),
        };

        assert!(payload.validate().is_err());
    }

    #[test]
    fn action_dispatch_variants_are_distinct() {
        let deploy = Action::Deploy(DeployPayload {
            release_name: "demo".to_string(),
            chart: "./chart".to_string(),
            namespace: "prod".to_string(),
            values: vec![],
            set: vec![],
        });
        let undeploy = Action::Undeploy(UndeployPayload {
            release_name: "demo".to_string(),
            namespace: "prod".to_string(),
        });

        assert_ne!(deploy, undeploy);
    }
}
