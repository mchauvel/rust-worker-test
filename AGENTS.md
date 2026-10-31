# Project Instructions

## Project Overview

This is a Rust 2024 asynchronous task worker. Multiple instances consume work from a shared RabbitMQ queue. Each instance also exposes a small HTTP API for local health and runner status.

The current implementation uses Tokio, Axum, and Lapin. Treat the code and `README.md` as the source of truth; do not describe planned behavior as implemented.

## Current Structure

- `src/main.rs` starts the application through the library entry point.
- `src/lib.rs` configures and runs the HTTP server and MQ consumer, including coordinated shutdown.
- `src/queue.rs` loads MQ and concurrency configuration, consumes messages, and acknowledges or dead-letters deliveries.
- `src/action/mod.rs` defines the finite action enum and dispatches to dedicated action modules in `src/action/`.
- `src/action/deploy.rs` and `src/action/undeploy.rs` contain the dedicated action handlers.
- `src/api.rs` defines the HTTP health and local runner-status endpoints.
- `src/status.rs` tracks the local MQ connection state and number of active tasks.

## Message Processing

- RabbitMQ settings: `AMQP_ADDR` defaults to `amqp://127.0.0.1:5672/%2f`; `AMQP_QUEUE` defaults to `tasks`.
- `WORKER_CONCURRENCY` sets the maximum in-flight tasks per process, defaults to `4`, and must be greater than zero. RabbitMQ prefetch is aligned with this limit. Total capacity is the per-process limit multiplied by the number of instances.
- Messages are JSON objects with a finite `action` value. `deploy` and `undeploy` are supported and carry structured payload fields such as `release_name`, `chart`, `namespace`, `values`, and `set`.
- Actions are dispatched through the `Action` enum to one handler file per action. `deploy` and `undeploy` invoke Helm directly with validated arguments and a bounded timeout; they do not build shell strings.
- Invalid messages and failed actions are rejected without requeue and routed to `<AMQP_QUEUE>.dead`. MQ connection or consumer errors are retried after five seconds.
- A delivery is acknowledged after its handler succeeds. This is at-least-once processing: a crash or connection loss can cause redelivery. Do not promise exactly-once execution. Make real actions idempotent.
- Competing RabbitMQ consumers distribute an unacknowledged delivery to one active consumer under normal operation. RabbitMQ alone cannot guarantee that an action will never run concurrently twice across a network partition. Strict exclusion requires a shared durable job registry with leases and fencing tokens.

When adding an action, add a typed enum variant, a dedicated handler module under `src/action/`, and focused tests. Keep message parameters structured and validated. Never turn message data into an arbitrary shell command. If Helm execution is added, invoke the executable with separate validated arguments, and design explicit timeout, cancellation, child-process cleanup, and least-privilege behavior.

## HTTP API

- `GET /health/live` checks that the process responds.
- `GET /health/ready` returns success when the local MQ consumer is connected and ready.
- `GET /api/v1/runners/self` reports this process only, including its local state and active-task count. It does not aggregate the fleet.
- `HTTP_ADDR` configures the listener and defaults to `127.0.0.1:8080`.

Do not present local health as fleet-wide status. A fleet view needs a shared source of truth.

## Development and Validation

Use stable Rust with Cargo. Run the relevant checks after Rust changes:

```powershell
cargo fmt --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

Keep changes focused, add tests for message parsing and action behavior, and document changes to configuration or message contracts. Do not add a persistence backend, distributed lock, retry policy, or new MQ topology without establishing the requirement and its delivery semantics first.