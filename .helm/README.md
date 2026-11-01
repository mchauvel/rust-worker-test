# Task Worker Helm chart

This chart installs the worker as a Kubernetes Deployment and exposes its local health/status API through a ClusterIP Service. The default image repository is a placeholder for the Artifactory OCI/Docker repository; override it and the image tag when installing.

## Install

Create a namespace-scoped Kubernetes Secret containing the settings required by the selected backend. For RabbitMQ, it can contain `AMQP_ADDR` and `AMQP_QUEUE`; for Elasticsearch, it can contain `ES_URL`, `ES_TASK_INDEX`, and the selected authentication variables. Do not put credentials in chart values or command-line `--set` arguments.

Create an image pull secret in the release namespace if Artifactory requires authentication, then install:

```powershell
helm upgrade --install task-worker .helm `
  --namespace task-worker `
  --create-namespace `
  --set image.repository=artifactory.example.com/docker-local/task-worker `
  --set-string image.tag=<image-tag> `
  --set imagePullSecrets[0].name=artifactory-pull `
  --set taskBackend=rabbitmq `
  --set extraEnvFrom[0].secretRef.name=task-worker-backend
```

`image.tag` must match an image already pushed to Artifactory. Use a values file for non-secret environment-specific settings instead of a long command line.

The Service is internal to the cluster. To inspect the API locally:

```powershell
kubectl -n task-worker port-forward svc/task-worker 8080:8080
```

Then use `http://127.0.0.1:8080/health/ready` or `http://127.0.0.1:8080/api/v1/runners/self`.

## Kubernetes permissions

The chart creates a ServiceAccount but deliberately creates no Role or ClusterRole. The worker invokes Helm from inside the Pod, so its ServiceAccount needs permissions to manage the resources installed by the task charts. Grant only the permissions and namespaces those charts require; avoid `cluster-admin`. If chart tasks target other namespaces, configure the corresponding bindings separately.

The Pod mounts the ServiceAccount token for in-cluster Helm authentication. The runtime filesystem is read-only except for temporary and task scratch volumes. The optional `file` backend uses ephemeral Pod storage in this chart; keep one replica for local experiments and do not use it as a durable or distributed production queue.

The image must include the Artifactory CA if internal TLS is used to fetch charts. The chart does not embed registry credentials or CA certificates.
