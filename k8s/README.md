# Kubernetes manifests

A kustomize base for running the server in a cluster, e.g. next to the Alquimia runtime on EKS
(Alquimia-ai/studio#258: brains connected to agents as MCP servers).

```bash
kubectl create namespace brains
kubectl -n brains apply -k k8s/base            # or reference k8s/base from an overlay
```

Override in an overlay: the image (`images:` in kustomization), `GBMS_*` values in
`configmap.yaml`, the PVC size/storage class and, **required**, the auth issuer list:
`GBMS_AUTH_ISSUERS: '[]'` in the base is a placeholder, and with auth enabled and no issuer the
server refuses to start (unless `GBMS_ALLOW_INSECURE_DEV=true`). Registry credentials (with push
rights), an embeddings API key and the cosign key path, when needed, go in the
`gaussia-brains-mcp-server` Secret (see `secret.example.yaml`); it is optional. A cosign public key
goes in a second optional Secret, `gaussia-brains-mcp-server-cosign` (key `cosign.pub`), which both
Deployments mount at `/keys`; then set `GBMS_COSIGN_KEY=/keys/cosign.pub`.

## The writer replica and its volume

This service owns every brain it writes: one OCI layout per brain on the
`gaussia-brains-mcp-server-store` PVC (`GBMS_STORE_ROOT=/var/lib/gbms/brains`). The Deployment is
the **single writer replica** (decision D11): `replicas: 1`, `strategy: Recreate`, and a
ReadWriteOnce volume. Writes to one brain are serialized in-process, and an `flock` on the volume
refuses a second process that mounts it by mistake. No Kubernetes Lease is used, so the base needs
no RBAC beyond its ServiceAccount.

`GBMS_STORE_ROOT` is set on the writer Deployment itself, not in the shared ConfigMap, so the
read replicas never inherit it. The writer uses `fsGroupChangePolicy: OnRootMismatch`, so a large
store is not re-chowned on every restart.

## Read replicas and routing

`deployment-read.yaml` / `service-read.yaml` run `gaussia-brains-mcp-server-read`: the same image,
**no** `GBMS_STORE_ROOT` and no volume (that is all "read-only mode" is), `replicas: 2`,
`RollingUpdate`. Scale it freely (or attach an HPA in an overlay). It answers every lifecycle write
with `refused_policy`.

Read replicas serve **published** brains only (proposal §17.2): they pull from the registry, so a
brain that exists only in the writer's working store (created or committed but not pushed) is not
visible there. Every response carries `X-GBMS-Role: writer` or `reader`, so a misrouted request is
easy to spot (a reader's lifecycle refusal also says `detail.replica: "reader"`). Route
accordingly:

| Traffic | Service |
|---------|---------|
| Lifecycle writes: `POST`/`DELETE` under `/v1/brains` (create, evidence, commits, branches, push, pull, converge, import, publish/unpublish) | `gaussia-brains-mcp-server` (writer) |
| `/v1/jobs` (job records live on the writer's volume) | writer |
| Browsing a working-store brain (Ledger: `GET` of commits, history, blocks; search on an unpushed brain) | writer |
| MCP (`/mcp/<brain-ref>`), discovery (`GET /v1/brains`), search of published brains | `gaussia-brains-mcp-server-read` |

With one ingress host, route by path and method (e.g. `/mcp` and Studio's discovery calls to the
read service, everything else to the writer). Both share one `GBMS_AUTH_RESOURCE`: they are one
logical resource, so a token's audience is valid on either. Point Studio's `BRAINS_MCP_BASE_URL`
at the read service to keep agent traffic off the writer.

Back up the PVC like any database volume: it is the working copy of record until a brain is
pushed. Push, pull, converge and import run as jobs whose records are kept on the same volume
(`$GBMS_STORE_ROOT/.jobs`, one JSON file per job, the newest `GBMS_JOBS_MAX_RETAINED`).

### Deploys and restarts

`strategy: Recreate` means **every deploy or restart of the writer is a short write outage**:
the old pod stops before the new one starts, and in between lifecycle writes fail (the read
replicas keep serving MCP and discovery). Plan deploys accordingly.

On SIGTERM the writer stops taking new jobs (`502 registry_unavailable`,
`detail.restarting: true` — clients retry shortly), cancels jobs that have not reached their
point of no return, and waits up to `GBMS_SHUTDOWN_GRACE_SECONDS` (default 25) for the ones that
have. `terminationGracePeriodSeconds: 60` leaves room for that before Kubernetes kills the pod.
Jobs survive as records: after the restart, `GET /v1/jobs/{id}` still answers, and a job that
was cut off reports `state: failed`, `stage: interrupted`, `error.code: interrupted` with
`error.detail.passed_commit_point` — `false` means nothing was changed (run it again), `true`
means it may have completed (re-read the brain's status before retrying). Nothing is left
half-written either way: blobs are content-addressed and pointers move last.

## How Alquimia reaches it

Each brain has its own MCP endpoint, `/mcp/<brain-ref>`. Studio builds it from
`BRAINS_MCP_BASE_URL`, which for this base is

```
http://gaussia-brains-mcp-server.brains.svc.cluster.local:8000
```

or, to send agents to the read replicas, `http://gaussia-brains-mcp-server-read.brains.svc.cluster.local:8000`.

The runtime (not Studio) opens those connections, so allow that host in the agentspace's egress
allow-list if one is set.

The Service is labelled `alquimia.ai/component=brains-mcp`, **not** `alquimia.ai/mcp-server=true`:
Studio's cluster MCP discovery registers a Service as a single MCP endpoint, and this server is
one endpoint per brain. Brains are connected through Studio's Brains flow instead.

## Auth

`GBMS_AUTH_ENABLED=true` (the default here) validates OIDC access tokens. In the platform bundle
the issuer is Keycloak: give the client an audience mapper for `GBMS_AUTH_RESOURCE`, and map realm
roles to scopes in `GBMS_AUTH_ISSUERS` (`role_scopes`). Ledger users need `brain-writer` →
`brains:write` (plus `brains:catalog`/`brains:read` to browse and search); writes are attributed
to the token's `email` (`GBMS_ACTOR_CLAIM`). With `GBMS_BRAIN_ACCESS=owners` (the default) a
user writes only the brains they created or were made a writer of; map an operator role to
`brains:admin` to write any brain, grant access and see every job. Agents need `brains:read`.

For MCP, the runtime presents the credential named by Studio's `BRAINS_MCP_AUTH_SECRET_REF` as a
bearer token, so that vault value has to be a token your issuer accepts for this resource
(`GBMS_AUTH_RESOURCE`). For a cluster-internal test without auth, set both
`GBMS_AUTH_ENABLED=false` and `GBMS_ALLOW_INSECURE_DEV=true` in an overlay.
