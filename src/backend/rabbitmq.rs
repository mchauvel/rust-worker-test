use std::{error::Error, sync::Arc, time::Duration};

use futures_lite::StreamExt;
use lapin::{
    Connection, ConnectionProperties,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicQosOptions, BasicRejectOptions,
        QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable, LongString, ShortString},
};
use tokio::{sync::watch, task::JoinSet};
use tracing::{debug, error, info, warn};

use crate::{
    action::{Action, dispatch},
    status::RunnerHealth,
    task_message::parse_action,
};

type BoxError = Box<dyn Error + Send + Sync>;

pub struct Config {
    address: String,
    queue_name: String,
    concurrency: u16,
}

impl Config {
    pub fn from_env() -> Result<Self, BoxError> {
        let address =
            std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".to_owned());
        let queue_name = std::env::var("AMQP_QUEUE").unwrap_or_else(|_| "tasks".to_owned());
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
            error!(amqp_addr = %config.address, queue_name = %config.queue_name, error = %error, "MQ connection or consumer failed; retrying in 5s");

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
    info!(amqp_addr = %config.address, queue_name = %config.queue_name, concurrency = %config.concurrency, "connecting to RabbitMQ");
    let connection = Connection::connect(&config.address, ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;
    channel
        .basic_qos(config.concurrency, BasicQosOptions::default())
        .await?;

    let dead_letter_queue = format!("{}.dead", config.queue_name);
    info!(queue_name = %config.queue_name, dead_letter_queue = %dead_letter_queue, "declaring RabbitMQ queues");
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
    info!(queue_name = %config.queue_name, concurrency = %config.concurrency, "consumer started; waiting for messages");
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

        debug!(
            payload_size = delivery.data.len(),
            "message received from queue"
        );
        let action = match parse_action(&delivery.data) {
            Ok(action) => action,
            Err(error) => {
                warn!(payload_size = delivery.data.len(), error = %error, "rejecting invalid task message");
                delivery
                    .reject(BasicRejectOptions { requeue: false })
                    .await?;
                health.set_waiting();
                continue;
            }
        };

        let action_label = match &action {
            Action::Deploy(_) => "deploy",
            Action::Undeploy(_) => "undeploy",
        };
        let task_health = Arc::clone(health);
        tasks.spawn(async move {
            let _active_task = ActiveTask::new(task_health);
            info!(action = action_label, "starting action execution");
            if let Err(error) = dispatch(action).await {
                error!(action = action_label, error = %error, "action execution failed; dead-lettering message");
                delivery
                    .reject(BasicRejectOptions { requeue: false })
                    .await?;
                return Ok(());
            }

            info!(action = action_label, "action executed successfully; acknowledging message");
            delivery.ack(BasicAckOptions::default()).await?;
            Ok::<(), BoxError>(())
        });
    }
}
