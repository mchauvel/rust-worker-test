# syntax=docker/dockerfile:1

FROM rust:1.82 AS builder
WORKDIR /src

COPY . .
RUN cargo build --release

FROM registry.redhat.io/ubi9/ubi:latest

# Install minimal OS dependencies
RUN dnf -y update && \
    dnf -y install curl ca-certificates tar gzip shadow-utils && \
    dnf clean all

# Install Helm
RUN curl -fsSL https://get.helm.sh/helm-v3.15.4-linux-amd64.tar.gz -o /tmp/helm.tar.gz && \
    tar -xzf /tmp/helm.tar.gz -C /tmp && \
    install /tmp/linux-amd64/helm /usr/local/bin/helm && \
    chmod +x /usr/local/bin/helm && \
    rm -rf /tmp/helm.tar.gz /tmp/linux-amd64

# Add non-root user
RUN useradd -r -u 1001 -m -s /sbin/nologin appuser

WORKDIR /app
COPY --from=builder /src/target/release/task-worker /app/task-worker
RUN chown -R appuser:appuser /app

USER appuser

ENV HTTP_ADDR=0.0.0.0:8080
ENV AMQP_ADDR=amqp://127.0.0.1:5672/%2f
ENV AMQP_QUEUE=tasks
ENV WORKER_CONCURRENCY=4
ENV HELM_BIN=/usr/local/bin/helm
ENV HELM_TIMEOUT_SECS=600

EXPOSE 8080

CMD ["/app/task-worker"]
