# Task Worker

Service Rust destiné à attendre des tâches et à les traiter de façon asynchrone. Plusieurs instances peuvent partager une source centralisée de tâches.

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
- `GET /health/ready` : retourne `200` lorsque le backend de tâches sélectionné est prêt, sinon `503`.
- `GET /api/v1/runners/self` : retourne l'état de cette instance locale (`connecting`, `waiting_for_messages`, `processing` ou `disconnected`) et le nombre de tâches actives.

L'adresse d'écoute se configure avec `HTTP_ADDR` (par défaut `127.0.0.1:8080`). L'état de toute la flotte nécessitera un registre partagé; ces endpoints ne prétendent pas agréger les autres instances.

## Backend de tâches

`TASK_BACKEND` choisit la source des tâches : `rabbitmq` (par défaut) ou `elasticsearch`. Les deux backends consomment le même contrat JSON `deploy` / `undeploy` décrit ci-dessous; ils ne doivent pas être activés simultanément pour le même ensemble de tâches.

## Consommation RabbitMQ

- `AMQP_ADDR` : URI AMQP du broker (par défaut `amqp://127.0.0.1:5672/%2f`).
- `AMQP_QUEUE` : nom de la file durable (par défaut `tasks`).
- `WORKER_CONCURRENCY` : nombre maximal de tâches simultanées par instance (par défaut `4`). Le prefetch RabbitMQ est aligné sur cette limite.

Le corps du message doit être un objet JSON avec un champ `action` égal à `deploy` ou `undeploy` et les paramètres attendus par l’action :

```json
{"action":"deploy","release_name":"demo","chart":"https://artifactory.example.com/artifactory/charts/sereca-1.2.3.tgz","namespace":"prod","values":["./values.yaml"],"set":["image.tag=1.2.3"]}
```

```json
{"action":"undeploy","release_name":"demo","namespace":"prod"}
```

Le champ `chart` peut être un chemin local, un dépôt Helm ou directement un tarball `.tgz` téléchargeable, par exemple un lien Artifactory. Le worker passe cette valeur telle quelle à Helm via `helm upgrade --install`, ce qui fonctionne pour les chart packagés en URL directe.

Chaque action est dispatchée vers son module dédié dans `src/action/`. `deploy` et `undeploy` invoquent désormais le binaire Helm avec des arguments séparés, sans passer par un shell, et avec un timeout explicite. Les messages invalides ou dont le traitement échoue sont rejetés vers la file de dead-letter `<AMQP_QUEUE>.dead`. Après une erreur de connexion, le consommateur réessaie toutes les cinq secondes.

Les variables d’environnement `HELM_BIN` (par défaut `helm`) et `HELM_TIMEOUT_SECS` (par défaut `600`) permettent de personnaliser le binaire Helm et la durée maximale d’exécution d’une action.

RabbitMQ distribue chaque livraison non acquittée à un seul consommateur actif et la limite de prefetch borne le travail en cours par instance. L'acquittement intervient après le traitement : une panne peut donc provoquer une redélivrance. Cette garantie est « au moins une fois », pas « exactement une fois »; les actions réelles devront être idempotentes. Une exclusion stricte même en cas de partition réseau demandera un registre de tâches partagé avec bail et fencing token.

## Backend Elasticsearch

Elasticsearch peut servir d'index de tâches avec réservation optimiste des documents. Configure :

- `ES_URL` : URL HTTP(S) de l'API Elasticsearch, par exemple `https://elasticsearch.example.com:9200`.
- `ES_TASK_INDEX` : index utilisé (par défaut `tasks`).
- Authentification facultative : `ES_API_KEY`, ou les deux variables `ES_USERNAME` et `ES_PASSWORD`.
- `WORKER_CONCURRENCY` : maximum de tâches simultanées pour cette instance.
- `ES_TASK_LEASE_SECS` : durée de réservation (par défaut `900`); doit être supérieure à `HELM_TIMEOUT_SECS`.
- `ES_MAX_ATTEMPTS` : nombre maximal de tentatives (par défaut `5`). Le délai de reprise double à chaque échec et est plafonné à cinq minutes. Une tâche épuisée passe à l'état `dead`.
- `ES_RETRY_DELAY_SECS` : délai de base avant la première reprise (par défaut `5`).

Créer l'index avec les champs de contrôle suivants :

```json
PUT tasks
{
	"mappings": {
		"properties": {
			"action": { "type": "keyword" },
			"release_name": { "type": "keyword" },
			"chart": { "type": "keyword" },
			"namespace": { "type": "keyword" },
			"values": { "type": "text" },
			"set": { "type": "text" },
			"status": { "type": "keyword" },
			"attempts": { "type": "long" },
			"locked_by": { "type": "keyword" },
			"lease_token": { "type": "keyword" },
			"lease_expires_at": { "type": "long" },
			"next_attempt_at": { "type": "long" },
			"updated_at": { "type": "long" },
			"last_error": { "type": "text" }
		}
	}
}
```

Un producteur crée les tâches avec `status: pending` et `attempts: 0`; l'identifiant Elasticsearch du document sert d'identifiant de tâche :

```json
PUT tasks/_doc/deploy-sereca-001
{
	"action": "deploy",
	"release_name": "sereca",
	"chart": "https://artifactory.example.com/artifactory/charts/sereca-1.2.3.tgz",
	"namespace": "prod",
	"status": "pending",
	"attempts": 0
}
```

Les états utilisés sont `pending`, `processing`, `completed` et `dead`. La réservation utilise `if_seq_no` / `if_primary_term`, puis un bail avec jeton propriétaire; un crash laisse le document récupérable à l'expiration du bail. La livraison reste « au moins une fois » : une panne après l'effet Helm mais avant le statut `completed` peut provoquer une nouvelle exécution. Les handlers doivent donc rester idempotents. Le rôle Elasticsearch du worker doit être limité aux opérations nécessaires sur cet index.

Pour le développement local, copier `.env.example` vers `.env` et modifier `TASK_BACKEND` et les paramètres du backend choisi. En CI et en production, injecter les secrets par l'environnement (variables GitLab / secrets du runtime), pas dans `.env` ni dans l'image.