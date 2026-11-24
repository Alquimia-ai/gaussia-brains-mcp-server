# Kubernetes manifests

A kustomize base for running the server in a cluster, e.g. next to the Alquimia runtime on EKS
(Alquimia-ai/studio#258: brains connected to agents as MCP servers).

```bash
kubectl create namespace brains
kubectl -n brains apply -k k8s/base            # or reference k8s/base from an overlay
```

Override in an overlay: the image (`images:` in kustomization), `GBMS_*` values in
`configmap.yaml`, the auth issuer, and the PVC size/storage class. Registry credentials (with push
rights), an embeddings API key and a cosign key, when needed, go in the `gaussia-brains-mcp-server`
Secret (see `secret.example.yaml`); it is optional.

## The writer replica and its volume

This service owns every brain it writes: one OCI layout per brain on the
`gaussia-brains-mcp-server-store` PVC (`GBMS_STORE_ROOT=/var/lib/gbms/brains`). The Deployment is
the **single writer replica** (decision D11): `replicas: 1`, `strategy: Recreate`, and a
ReadWriteOnce volume. Writes to one brain are serialized in-process, and an `flock` on the volume
refuses a second process that mounts it by mistake. No Kubernetes Lease is used, so the base needs
no RBAC beyond its ServiceAccount.

To scale reads (MCP, discovery, search of published brains), run a second Deployment from the
same image **without** `GBMS_STORE_ROOT` and without the volume; it answers every lifecycle write
with `refused_policy`. Route `/v1/brains` writes and `/v1/jobs` to the writer.

Back up the PVC like any database volume: it is the working copy of record until a brain is
pushed. Push, pull, converge and import run as in-memory jobs; a restart cancels running ones
safely (blobs are content-addressed and pointers move last) and forgets finished ones.

## How Alquimia reaches it

Each brain has its own MCP endpoint, `/mcp/<brain-ref>`. Studio builds it from
`BRAINS_MCP_BASE_URL`, which for this base is

```
http://gaussia-brains-mcp-server.brains.svc.cluster.local:8000
```

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
to the token's `email` (`GBMS_ACTOR_CLAIM`). Agents need `brains:read`.

For MCP, the runtime presents the credential named by Studio's `BRAINS_MCP_AUTH_SECRET_REF` as a
bearer token, so that vault value has to be a token your issuer accepts for this resource
(`GBMS_AUTH_RESOURCE`). For a cluster-internal test without auth, set both
`GBMS_AUTH_ENABLED=false` and `GBMS_ALLOW_INSECURE_DEV=true` in an overlay.
