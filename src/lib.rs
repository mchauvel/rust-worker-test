mod action;
mod api;
mod queue;
mod status;

use std::net::SocketAddr;
use std::sync::Arc;

use tracing_subscriber::{EnvFilter, fmt};

pub async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .try_init();

    let address = std::env::var("HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_owned())
        .parse::<SocketAddr>()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    let queue_config = queue::Config::from_env()?;
    let health = Arc::new(status::RunnerHealth::new());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    tracing::info!(
        http_addr = %address,
        amqp_addr = %queue_config.address(),
        queue_name = %queue_config.queue_name(),
        concurrency = %queue_config.concurrency(),
        "starting task worker"
    );

    let signal_tx = shutdown_tx.clone();
    let server = axum::serve(listener, api::router(Arc::clone(&health))).with_graceful_shutdown(
        async move {
            let _ = tokio::signal::ctrl_c().await;
            let _ = signal_tx.send(true);
        },
    );
    let worker = tokio::spawn(queue::run(queue_config, health, shutdown_rx));

    let server_result = server.await;
    let _ = shutdown_tx.send(true);
    worker.await??;
    server_result?;

    Ok(())
}
