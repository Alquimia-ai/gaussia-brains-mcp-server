# Gaussia Brains MCP Server

Serves [Boltzmann Brains](https://pypi.org/project/pyboltzmann/) to agents over MCP: headless, read-only, with optional publisher-signature verification. It's a standalone service with its own release lifecycle and no Alquimia dependencies.

1. A publisher pushes a brain to an OCI registry and adds it to the namespace index.
2. A user lists brains (`GET /v1/brains`), sees signature status, and picks one for an agent — pinned to a digest or following a tag.
3. The agent connects to that brain's MCP endpoint and gets `brain_search`, `brain_read_block`, and `brain_resolve`. The server generates instructions and prompts that explain how to use them. There is no listing and no modification over MCP.

## Status

Implemented and tested:

- Per-brain MCP over Streamable HTTP (`/mcp/<brain-ref>`).
- REST discovery and management endpoints (`/v1/brains`).
- Configurable short brain references such as `brains/my-brain:latest`.
- OCI registry client (`oras`), brain cache, and namespace-index publishing.
- Optional cosign signature verification (`GBMS_SIGNATURE_POLICY=off|warn|required`).

## Development

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e ".[dev]"
.venv/bin/pytest
.venv/bin/ruff check src tests
```

Runtime requirements:

- Python 3.12
- The `oras` CLI
- `cosign` when `GBMS_SIGNATURE_POLICY` is not `off`

Run the end-to-end smoke test against the public `registry.alquimia.ai/brains` namespace:

```bash
.venv/bin/python e2e_smoke.py
```

## Key settings

```bash
# Where brains may come from (fail closed when empty)
GBMS_CATALOG_NAMESPACES='["registry.alquimia.ai/brains"]'
GBMS_ALLOWED_SOURCES='["registry.alquimia.ai/brains"]'

# Default registry used to expand short refs like brains/name:tag
GBMS_REGISTRY_DEFAULT=registry.alquimia.ai
GBMS_REGISTRY_NAMESPACE_BRAINS=brains

# Publisher-signature verification (feature flag: off | warn | required)
GBMS_SIGNATURE_POLICY=off
# GBMS_TRUSTED_SIGNERS='[{"issuer": "https://token.actions.githubusercontent.com",
#                         "subject_regex": "https://github\\.com/acme/knowledge/.+"}]'

# Who may call (OAuth 2.0 resource server, any OIDC issuer)
GBMS_AUTH_RESOURCE=https://brains.example.com
GBMS_AUTH_ISSUERS='[{"issuer": "https://sso.example.com/realms/acme",
                     "claims": {"roles": ["realm_access.roles"]},
                     "role_scopes": {"brain-reader": ["brains:read"],
                                     "brain-catalog": ["brains:catalog"],
                                     "brain-publisher": ["brains:publish"]}}]'
```

| Scope | Grants |
|-------|--------|
| `brains:read` | Connect to any servable brain over MCP and use its tools |
| `brains:catalog` | `GET /v1/brains` and `GET /v1/brains/{brain_ref}` |
| `brains:publish` | `POST /v1/brains/reindex`, `POST /v1/brains/{brain_ref}/publish`, and `POST /v1/brains/{brain_ref}/unpublish` |

To run locally without auth, set both `GBMS_AUTH_ENABLED=false` and `GBMS_ALLOW_INSECURE_DEV=true`.

## REST API

All brain-reference parameters accept short forms such as `brains/my-brain:latest` and expand them using `GBMS_REGISTRY_DEFAULT` and `GBMS_REGISTRY_NAMESPACE_BRAINS`.

| Method | Path | Scope | Description |
|--------|------|-------|-------------|
| `GET` | `/v1/brains` | `brains:catalog` | List brains from namespace indexes |
| `GET` | `/v1/brains/{brain_ref}` | `brains:catalog` | Manifest-level detail for one brain |
| `POST` | `/v1/brains/reindex` | `brains:publish` | Reindex a namespace or a list of refs |
| `POST` | `/v1/brains/{brain_ref}/publish` | `brains:publish` | Attach a `kind=brain` entry to the namespace index |
| `POST` | `/v1/brains/{brain_ref}/unpublish` | `brains:publish` | Detach the index entry for a brain |

Query parameters for `GET /v1/brains`:

- `namespace` — read this namespace instead of the configured catalog namespaces.
- `limit` — page size (1–100, default 20).
- `cursor` — opaque pagination cursor from a previous response.

## MCP endpoints

The MCP server is mounted under the configured `GBMS_MCP_PATH` (default `/mcp`). Each brain gets its own URL:

```text
/mcp/brains/my-brain:latest
/mcp/registry.example.com/acme/handbook@sha256:abc123...
```

The transport is Streamable HTTP. Each endpoint exposes:

- `brain_search` — semantic/procedural search over the brain.
- `brain_read_block` — fetch and return a single block by id.
- `brain_resolve` — manifest, digest, modules, and signature status.
- Prompts: `answer_from_brain`, `explore_brain`.

## Brain references

Short references are expanded with the configured defaults:

| Input | Expanded (defaults) |
|-------|---------------------|
| `brains/my-brain:latest` | `registry.alquimia.ai/brains/my-brain:latest` |
| `my-brain` | `registry.alquimia.ai/brains/my-brain:latest` |
| `registry.example.com/acme/handbook:main` | unchanged |

Digest pinning is supported: `brains/my-brain@sha256:...`.
