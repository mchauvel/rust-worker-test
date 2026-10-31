use std::{
    error::Error,
    sync::Arc,
    time::Duration,
};

use futures_lite::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicQosOptions, BasicRejectOptions,
        QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable, LongString, ShortString},
    Connection, ConnectionProperties,
};
use serde::Deserialize;
use tokio::{sync::watch, task::JoinSet};

use crate::{
    action::{dispatch, Action},
    status::RunnerHealth,
};

type BoxError = Box<dyn Error + Send + Sync>;

pub struct Config {
    address: String,
    queue_name: String,
    concurrency: u16,
}

impl Config {
    pub fn from_env() -> Result<Self, BoxError> {
        let address = std::env::var("AMQP_ADDR")
            .unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".to_owned());
        let queue_name = std::env::var("AMQP_QUEUE")
            .unwrap_or_else(|_| "tasks".to_owned());
        let concurrency = std::env::var("WORKER_CONCURRENCY")
            .unwrap_or_else(|_| "4".to_owned())
            .parse::<u16>()?;

        if queue_name.trim().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "AMQP_QUEUE must not be empty",
            )
            .into());
        }
        if concurrency == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "WORKER_CONCURRENCY must be greater than zero",
            )
            .into());
        }

        Ok(Self {
            address,
            queue_name,
            concurrency,
        })
    }
}

#[derive(Deserialize)]
struct TaskMessage {
    action: Action,
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
    loop {
        if *shutdown.borrow() {
            break;
        }

        health.set_connecting();
        if let Err(error) = consume(&config, &health, &mut shutdown).await {
            health.set_disconnected();
            eprintln!("MQ connection or consumer failed: {error}; retrying in 5s");

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                _ = shutdown.changed() => break,
            }
        } else {
            break;
        }
    }

    Ok(())
}

async fn consume(
    config: &Config,
    health: &Arc<RunnerHealth>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), BoxError> {
    let connection = Connection::connect(&config.address, ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;
    channel
        .basic_qos(config.concurrency, BasicQosOptions::default())
        .await?;

    let dead_letter_queue = format!("{}.dead", config.queue_name);
    channel
        .queue_declare(
            dead_letter_queue.clone().into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    let mut queue_arguments = FieldTable::default();
    queue_arguments.insert(
        ShortString::from("x-dead-letter-exchange"),
        AMQPValue::LongString(LongString::from("")),
    );
    queue_arguments.insert(
        ShortString::from("x-dead-letter-routing-key"),
        AMQPValue::LongString(LongString::from(dead_letter_queue)),
    );

    channel
        .queue_declare(
            config.queue_name.clone().into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            queue_arguments,
        )
        .await?;

    let mut consumer = channel
        .basic_consume(
            config.queue_name.clone().into(),
            format!("task-worker-{}", std::process::id()).into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    health.set_waiting();
    println!(
        "Consuming RabbitMQ queue '{}' with concurrency {}.",
        config.queue_name, config.concurrency
    );
    let mut tasks = JoinSet::new();

    loop {
        if *shutdown.borrow() {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            return Ok(());
        }

        if tasks.len() >= usize::from(config.concurrency) {
            tokio::select! {
                _ = shutdown.changed() => continue,
                Some(result) = tasks.join_next() => result??,
            }
            continue;
        }

        let next = tokio::select! {
            _ = shutdown.changed() => continue,
            result = consumer.next() => result,
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                result??;
                continue;
            }
        };

        let Some(delivery) = next else {
            return Err("MQ consumer stream ended".into());
        };
        let delivery = delivery?;

        let message = match parse_action(&delivery.data) {
            Ok(message) => message,
            Err(error) => {
                eprintln!("Rejecting invalid task message: {error}");
                delivery
                    .reject(BasicRejectOptions { requeue: false })
                    .await?;
                health.set_waiting();
                continue;
            }
        };

        let task_health = Arc::clone(&health);
        tasks.spawn(async move {
            let _active_task = ActiveTask::new(task_health);
            if let Err(error) = dispatch(message.action).await {
                eprintln!("Task failed and will be dead-lettered: {error}");
                delivery
                    .reject(BasicRejectOptions { requeue: false })
                    .await?;
                return Ok(());
            }

            delivery.ack(BasicAckOptions::default()).await?;
            Ok::<(), BoxError>(())
        });
    }
}

fn parse_action(payload: &[u8]) -> Result<TaskMessage, String> {
    serde_json::from_slice(payload).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::parse_action;
    use crate::action::Action;

    #[test]
    fn parses_action_from_json_message() {
        let message = parse_action(br#"{"action":"deploy"}"#).unwrap();
        assert_eq!(message.action, Action::Deploy);
    }

    #[test]
    fn rejects_unknown_action() {
        assert!(parse_action(br#"{"action":"unknown"}"#).is_err());
    }

    #[test]
    fn parses_undeploy_action() {
        let message = parse_action(br#"{"action":"undeploy"}"#).unwrap();
        assert_eq!(message.action, Action::Undeploy);
    }
}