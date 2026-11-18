# Kubernetes manifests

A kustomize base for running the server in a cluster, e.g. next to the Alquimia runtime on EKS
(Alquimia-ai/studio#258: brains connected to agents as MCP servers).

```bash
kubectl create namespace brains
kubectl -n brains apply -k k8s/base            # or reference k8s/base from an overlay
```

Override in an overlay: the image (`images:` in kustomization), `GBMS_*` values in
`configmap.yaml`, and the auth issuer. Registry credentials and a cosign key, when needed, go in
the `gaussia-brains-mcp-server` Secret (see `secret.example.yaml`); it is optional.

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

`GBMS_AUTH_ENABLED=true` (the default here) validates OIDC access tokens with the `brains:read`
scope. The runtime presents the credential named by Studio's `BRAINS_MCP_AUTH_SECRET_REF` as a
bearer token, so that vault value has to be a token your issuer accepts for this resource
(`GBMS_AUTH_RESOURCE`). For a cluster-internal test without auth, set both
`GBMS_AUTH_ENABLED=false` and `GBMS_ALLOW_INSECURE_DEV=true` in an overlay.
