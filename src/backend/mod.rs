use std::error::Error;

use tokio::sync::watch;

use crate::status::RunnerHealth;

mod elasticsearch;
mod rabbitmq;

type BoxError = Box<dyn Error + Send + Sync>;

pub enum Config {
    RabbitMq(rabbitmq::Config),
    Elasticsearch(elasticsearch::Config),
}

#[derive(Debug, PartialEq, Eq)]
enum BackendKind {
    RabbitMq,
    Elasticsearch,
}

fn parse_backend(value: Option<&str>) -> Result<BackendKind, String> {
    match value.unwrap_or("rabbitmq").to_ascii_lowercase().as_str() {
        "rabbitmq" => Ok(BackendKind::RabbitMq),
        "elasticsearch" => Ok(BackendKind::Elasticsearch),
        backend => Err(format!(
            "unsupported TASK_BACKEND '{backend}'; expected 'rabbitmq' or 'elasticsearch'"
        )),
    }
}

impl Config {
    pub fn from_env() -> Result<Self, BoxError> {
        match parse_backend(std::env::var("TASK_BACKEND").ok().as_deref())? {
            BackendKind::RabbitMq => Ok(Self::RabbitMq(rabbitmq::Config::from_env()?)),
            BackendKind::Elasticsearch => {
                Ok(Self::Elasticsearch(elasticsearch::Config::from_env()?))
            }
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::RabbitMq(_) => "rabbitmq",
            Self::Elasticsearch(_) => "elasticsearch",
        }
    }

    pub async fn run(
        self,
        health: std::sync::Arc<RunnerHealth>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), BoxError> {
        match self {
            Self::RabbitMq(config) => rabbitmq::run(config, health, shutdown).await,
            Self::Elasticsearch(config) => elasticsearch::run(config, health, shutdown).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BackendKind, parse_backend};

    #[test]
    fn config_defaults_to_rabbitmq() {
        assert_eq!(parse_backend(None).unwrap(), BackendKind::RabbitMq);
    }

    #[test]
    fn backend_names_are_case_insensitive() {
        assert_eq!(
            parse_backend(Some("Elasticsearch")).unwrap(),
            BackendKind::Elasticsearch
        );
    }

    #[test]
    fn unsupported_backend_is_rejected() {
        assert!(parse_backend(Some("redis")).is_err());
    }
}
