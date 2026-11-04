# Gaussia Brains MCP Server

Serves [Boltzmann Brains](https://pypi.org/project/pyboltzmann/) to agents over MCP: headless, signed, read-only. It's a standalone service with its own release lifecycle and no Alquimia dependencies.

1. A publisher pushes a brain to an OCI registry, signs it **keyless** with their OIDC identity (Sigstore), and adds it to the namespace index.
2. A user lists brains (`GET /v1/brains`), sees **who signed each one** (verified by this server), and picks one for an agent, pinned to a digest or following a tag.
3. The agent connects to that brain's MCP endpoint and gets `brain_search`, `brain_read_block` and `brain_resolve`. The server generates the instructions and prompts that explain how to use them. There's no listing and no modification over MCP.

## Status

The foundation is implemented and tested: JWT auth, signature verification with a signer allow-list, registry client and brain cache. The MCP server, discovery API and search-index port are next. See [`docs/proposal.md`](docs/proposal.md) §13.

## Development

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e ".[dev]"
.venv/bin/pytest
.venv/bin/ruff check src tests
```

Runtime requirements: the `oras` CLI, plus `cosign` when `GBMS_SIGNATURE_POLICY` is not `off`.

## Key settings

```bash
# Where brains may come from (fail closed when empty)
GBMS_CATALOG_NAMESPACES='["registry.example.com/acme"]'

# Which publishers are trusted (feature flag: off | warn | required)
GBMS_SIGNATURE_POLICY=required
GBMS_TRUSTED_SIGNERS='[{"issuer": "https://token.actions.githubusercontent.com",
                        "subject_regex": "https://github\\.com/acme/knowledge/.+"}]'

# Who may call (OAuth 2.0 resource server, any OIDC issuer)
GBMS_AUTH_RESOURCE=https://brains.example.com
GBMS_AUTH_ISSUERS='[{"issuer": "https://sso.example.com/realms/acme",
                     "claims": {"roles": ["realm_access.roles"]},
                     "role_scopes": {"brain-reader": ["brains:read"],
                                     "brain-catalog": ["brains:catalog"]}}]'
```

| Scope | Grants |
|-------|--------|
| `brains:read` | Connect to any servable brain over MCP and use its tools |
| `brains:catalog` | `GET /v1/brains` |

To run locally without auth, set both `GBMS_AUTH_ENABLED=false` and `GBMS_ALLOW_INSECURE_DEV=true`.
