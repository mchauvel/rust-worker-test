mod action;
mod api;
mod queue;
mod status;

use std::net::SocketAddr;
use std::sync::Arc;

pub async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let address = std::env::var("HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_owned())
        .parse::<SocketAddr>()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    let queue_config = queue::Config::from_env()?;
    let health = Arc::new(status::RunnerHealth::new());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let signal_tx = shutdown_tx.clone();
    let server = axum::serve(listener, api::router(Arc::clone(&health)))
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            let _ = signal_tx.send(true);
        });
    let worker = tokio::spawn(queue::run(queue_config, health, shutdown_rx));

    let server_result = server.await;
    let _ = shutdown_tx.send(true);
    worker.await??;
    server_result?;

    Ok(())
}