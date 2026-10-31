# Task Worker

Service Rust destiné à attendre des tâches et à les traiter de façon asynchrone. Plusieurs instances doivent pouvoir partager une file centralisée.

Le dépôt contient le squelette initial du package. Le traitement des tâches et le backend de la file ne sont pas encore implémentés. Le choix du stockage, la durabilité, les garanties de livraison, l'idempotence et la politique de retry restent à définir avant de fixer les contrats de la file.

## Prérequis

- Rust stable avec Cargo

## Développement

```powershell
cargo run
cargo test
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

Le code applicatif est séparé entre une bibliothèque (`src/lib.rs`), utilisable et testable indépendamment, et un exécutable (`src/main.rs`). Tokio fournit le runtime asynchrone multi-thread.

## API de supervision

- `GET /health/live` : confirme que le processus répond.
- `GET /health/ready` : retourne `200` lorsque le consommateur MQ est prêt, sinon `503`.
- `GET /api/v1/runners/self` : retourne l'état de cette instance locale (`connecting`, `waiting_for_messages`, `processing` ou `disconnected`) et le nombre de tâches actives.

L'adresse d'écoute se configure avec `HTTP_ADDR` (par défaut `127.0.0.1:8080`). L'état de toute la flotte nécessitera un registre partagé; ces endpoints ne prétendent pas agréger les autres instances.

## Consommation RabbitMQ

- `AMQP_ADDR` : URI AMQP du broker (par défaut `amqp://127.0.0.1:5672/%2f`).
- `AMQP_QUEUE` : nom de la file durable (par défaut `tasks`).
- `WORKER_CONCURRENCY` : nombre maximal de tâches simultanées par instance (par défaut `4`). Le prefetch RabbitMQ est aligné sur cette limite.

Le corps du message doit être un objet JSON avec un champ `action` égal à `deploy` ou `undeploy`, par exemple :

```json
{"action":"deploy"}
```

Chaque action est dispatchée vers son module dédié dans `src/action/`. `deploy` et `undeploy` affichent actuellement un message placeholder; `deploy` ne lance pas encore Helm. Les messages invalides ou dont le traitement échoue sont rejetés vers la file de dead-letter `<AMQP_QUEUE>.dead`. Après une erreur de connexion, le consommateur réessaie toutes les cinq secondes.

RabbitMQ distribue chaque livraison non acquittée à un seul consommateur actif et la limite de prefetch borne le travail en cours par instance. L'acquittement intervient après le traitement : une panne peut donc provoquer une redélivrance. Cette garantie est « au moins une fois », pas « exactement une fois »; les actions réelles devront être idempotentes. Une exclusion stricte même en cas de partition réseau demandera un registre de tâches partagé avec bail et fencing token.