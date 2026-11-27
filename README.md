# Gaussia Brains MCP Server

The only owner of [Boltzmann Brains](https://pypi.org/project/pyboltzmann/) on the Alquimia platform: people build brains over REST, agents read them over MCP. It's a standalone service with its own release lifecycle and no Alquimia dependencies.

1. A user (through Ledger, or any REST client) creates a brain, registers evidence, commits the knowledge a model proposed from it, branches, converges and pushes — all under their own OIDC token, against the service's working store.
2. Pushing `main` publishes the brain to its OCI registry and adds it to the namespace index.
3. A user lists brains (`GET /v1/brains`) and picks one for an agent — pinned to a digest or following a tag.
4. The agent connects to that brain's MCP endpoint and gets `brain_search` and `brain_read_block`. The server generates instructions and prompts that explain how to use them. MCP stays read-only: no listing and no modification.

## Status

Implemented and tested:

- Per-brain MCP over Streamable HTTP (`/mcp/<brain-ref>`).
- REST discovery and management endpoints (`/v1/brains`).
- The full brain lifecycle over REST: working store per brain, evidence, commits, browse, history, branches, push/pull, converge, import, jobs (see [the lifecycle API](#lifecycle-api)).
- Search engines ported from core (hash map, BM25 with accent folding, B+ tree, graph, vector via an OpenAI-compatible embeddings endpoint; bitmap is ported but, as in core, not registered), shared by MCP and REST.
- Configurable short brain references such as `brains/my-brain:latest`.
- OCI registry client (`oras`), brain cache, and namespace-index publishing.
- Optional cosign signature verification (`GBMS_SIGNATURE_POLICY=off|warn|required`).

## Development

```bash
uv venv --python 3.13 .venv
uv pip install --python .venv/bin/python -e ".[dev]"
.venv/bin/pytest
.venv/bin/ruff check src tests
```

Runtime requirements:

- Python 3.13
- The `oras` CLI (registry credentials with push rights on the writer replica)
- `cosign` when `GBMS_SIGNATURE_POLICY` is not `off`
- A volume for `GBMS_STORE_ROOT` on the writer replica

The REST contract is `docs/openapi.json`, generated from the app. Regenerate it after changing an endpoint (a test fails when it is stale):

```bash
.venv/bin/python scripts/export_openapi.py
```

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

# Discovery reads the namespace index anchor (<ns>/index:v1) in one call.
# Seconds a listing is reused (publish/unpublish here invalidate it; 0 = always read)
GBMS_CATALOG_INDEX_TTL_SECONDS=10
# Entries enriched in parallel (legacy entries without a modules annotation, signatures)
GBMS_CATALOG_CONCURRENCY=8

# The working store: one OCI layout per brain this replica writes (a volume).
# Unset = read-only replica (MCP, discovery and search only).
GBMS_STORE_ROOT=/var/lib/gbms/brains
# Writes are attributed to the token's human: this claim if it is an address,
# else <GBMS_ACTOR_NAMESPACE or issuer host>/<sub>
GBMS_ACTOR_CLAIM=email
# Who may write a held brain: owners (its creator + the writers it lists, or a
# brains:admin token; the default) | open (any brains:write holder)
GBMS_BRAIN_ACCESS=owners
# Job records persist under $GBMS_STORE_ROOT/.jobs (the newest N are kept)
GBMS_JOBS_MAX_RETAINED=500
# On shutdown, seconds jobs past their point of no return get to finish
GBMS_SHUTDOWN_GRACE_SECONDS=25

# Semantic search (optional): any OpenAI-compatible /v1/embeddings endpoint
GBMS_EMBEDDINGS_URL=http://localhost:8001/v1
GBMS_EMBEDDINGS_MODEL=text-embedding-3-small
# GBMS_EMBEDDINGS_MODEL_TAG=openai/text-embedding-3-small   # must match published vector layers

# Signing and governance are deferred: new brains are ungoverned, unsigned
# snapshots are served with a warning, governed brains are read-only.
GBMS_VERIFY_UNSIGNED=warn
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
                                     "brain-publisher": ["brains:publish"],
                                     "brain-writer": ["brains:write", "brains:catalog", "brains:read"],
                                     "brain-admin": ["brains:admin", "brains:write", "brains:catalog", "brains:read"]}}]'
```

| Scope | Grants |
|-------|--------|
| `brains:read` | Connect to any servable brain over MCP and use its tools; `POST /v1/brains/{brain_ref}/search` |
| `brains:catalog` | `GET /v1/brains` and `GET /v1/brains/{brain_ref}`; browsing working-store brains |
| `brains:publish` | `POST /v1/brains/reindex`, `POST /v1/brains/{brain_ref}/publish`, and `POST /v1/brains/{brain_ref}/unpublish` |
| `brains:write` | Every lifecycle write (create, evidence, commits, branches, push, pull, converge, import), `GET /v1/brains?held=true` and the caller's own `/v1/jobs`. With `GBMS_BRAIN_ACCESS=owners` (default) only on brains the caller created or is a writer of; with `open`, on any brain in an allowed source |
| `brains:admin` | Write any held brain whatever its owners, grant write access to any brain (`PUT /v1/brains/{ref}/writers`), settle any open convergence, list every held brain, and see and cancel every job. Writes still need `brains:write` |

To run locally without auth, set both `GBMS_AUTH_ENABLED=false` and `GBMS_ALLOW_INSECURE_DEV=true`.

## REST API

All brain-reference parameters accept short forms such as `brains/my-brain:latest` and expand them using `GBMS_REGISTRY_DEFAULT` and `GBMS_REGISTRY_NAMESPACE_BRAINS`.

| Method | Path | Scope | Description |
|--------|------|-------|-------------|
| `GET` | `/v1/brains` | `brains:catalog` | List brains from namespace indexes |
| `GET` | `/v1/brains?held=true` | `brains:catalog` or `brains:write` | List brains held in the working store (see below) |
| `GET` | `/v1/brains/{brain_ref}` | `brains:catalog` | Manifest-level detail for one brain |
| `POST` | `/v1/brains/reindex` | `brains:publish` | Reindex a namespace or a list of refs |
| `POST` | `/v1/brains/{brain_ref}/publish` | `brains:publish` | Attach a `kind=brain` entry to the namespace index |
| `POST` | `/v1/brains/{brain_ref}/unpublish` | `brains:publish` | Detach the index entry for a brain |

Query parameters for `GET /v1/brains`:

- `namespace` — read this namespace instead of the configured catalog namespaces.
- `limit` — page size (1–100, default 20).
- `cursor` — opaque pagination cursor from a previous response.
- `held=true` — list the brains in this service's working store instead, ordered by reference and paginated with `limit`/`cursor` (`next_cursor` is `null` on the last page). Under `GBMS_BRAIN_ACCESS=owners` only the brains the caller may write are listed (every one with `brains:admin`); each entry carries `owner`, `writers` and `writable`.

Every HTTP response carries `X-GBMS-Role: writer` (this replica holds the working store) or `reader` (a read replica; lifecycle routes answer `refused_policy` with `detail.replica == "reader"`).

## Lifecycle API

Writes need `brains:write` and are attributed to the token's human. Push, pull, converge and import answer `202` with a job (`GET /v1/jobs/{id}`, `POST /v1/jobs/{id}/cancel`). The full design is [docs/proposal.md §17](docs/proposal.md#17-the-brain-lifecycle-over-rest-issue-2); the contract is [docs/openapi.json](docs/openapi.json).

| Method | Path (under `/v1/brains`) | Description |
|--------|------|-------------|
| `POST` | `` | Create an empty, ungoverned brain (`reference`, `default_tag`, `title`, `description`) |
| `GET` | `/{ref}` | Status of a held brain: modules and counts, head (`ETag`), branches, open convergence, publication |
| `GET` | `/{ref}/modules/{mt}/blocks` | Blocks newest first, superseded and unreadable included (`branch`, `cursor`, `limit`) |
| `GET` | `/{ref}/blocks/{id}` | One block: provenance, supersession, verification, extracted text |
| `GET` | `/{ref}/history` | Snapshots of a branch, newest first |
| `POST` | `/{ref}/search` | The MCP search engine; reports `truncated` and `narrowing` |
| `GET`/`PUT` | `/{ref}/writers` | Who may write besides the owner: `{"reference", "owner", "writers", "access"}`; `PUT {"writers": [actor ids]}` replaces the list (owner or `brains:admin` only) |
| `POST` | `/{ref}/evidence` | Register a source (multipart `file`) |
| `POST` | `/{ref}/commits` | Validate and commit client-proposed candidates, all or nothing (`dry_run`); the source is `source` or `evidence_inline` |
| `GET`/`POST`/`DELETE` | `/{ref}/branches[/{name}]` | List (`remote=true` for published ones), create, delete (`force=true` for an unmerged branch) |
| `POST` | `/{ref}/branches/{name}/push` | Push a branch to its tag (job); main also indexes |
| `POST` | `/{ref}/pull` | Install a published tag (job) |
| `POST` | `/{ref}/converge` | Bring a branch or tag into a branch (`from`, `into`, `strategy`, `reason`) (job) |
| `GET`/`POST` | `/{ref}/convergence[/resolutions\|/accept-removals\|/continue\|/abort]` | Inspect and settle an open convergence (optional `branch` it must be open on) |
| `POST` | `/{ref}/import` | Migrate a brain from the registry (JSON) or an uploaded OCI layout (multipart `layout`) (job) |

Writes accept `If-Match: <head digest>` (from `ETag`) and are refused with `409 not_fast_forward` when the branch moved. Errors are `{"code", "message", "detail"}` with `code` one of `usage`, `not_found`, `refused_protocol`, `refused_policy`, `rejected_candidates`, `not_fast_forward`, `registry_unavailable`, `needs_human` (and `unauthenticated`, `internal`); [`lifecycle/errors.py`](src/gaussia_brains_mcp_server/lifecycle/errors.py) says what each means.

**Who may write.** With `GBMS_BRAIN_ACCESS=owners` (the default) a brain is written only by its owner (`created_by`: whoever created or imported it), the writers it lists, or a `brains:admin` token; anyone else gets `403 refused_policy` with `detail: {"access": "owners", "owners": [owner, ...writers]}` and a message naming who can grant access. A brain with no recorded owner is writable only by an admin until writers are set. `GET /v1/brains/{ref}` reports `owner`, `writers` and `writable` (for the caller). `GBMS_BRAIN_ACCESS=open` lets any `brains:write` holder write any brain.

**Jobs.** A caller sees, lists and cancels only its own jobs (`actor`); someone else's job is `404 not_found`. `brains:admin` sees every job. Job records are persisted on the writer's volume (`$GBMS_STORE_ROOT/.jobs`, the newest `GBMS_JOBS_MAX_RETAINED`) and survive a restart: a job that was running when the service stopped is reported `state: failed`, `stage: interrupted`, `error.code: interrupted`, with `error.detail.passed_commit_point` — `false` means nothing was changed (run it again), `true` means it may have completed (re-read the brain's status first). `cancelled` and `interrupted` are job error codes, never HTTP ones. On shutdown the service refuses new jobs (`502 registry_unavailable`, `detail: {"restarting": true}`), cancels jobs still before their point of no return and waits up to `GBMS_SHUTDOWN_GRACE_SECONDS` for the others.

**One convergence per brain.** pyboltzmann keeps one head pointer per brain and refuses every write while a reconciliation is open, so a convergence left open (a converge job that ended `needs_human`) blocks writes to *every* branch. Such a write answers `409 needs_human` with `detail: {"convergence": {"branch", "actor", "theirs", ...}}`. `GET /{ref}/convergence` reports `branch`, `actor` (who opened it), `theirs` (what it joins), `strategy`, `reason` and `started_at`. Only its actor, the brain's owner or `brains:admin` may resolve, accept removals, continue or abort it (`refused_policy` otherwise; any writer under `open`); each of these accepts an optional `branch` (in the JSON body; continue/abort also work without one) and answers `400 usage` if the convergence is open on another branch.

**Evidence and commit in one write.** `POST /{ref}/commits` takes either `source` (a registered block) or `evidence_inline: {"data_base64", "media_type", "filename", "origin"}`. With inline evidence the candidates are validated against the brain as it would be with the evidence added — without writing it — under one write lock and one `If-Match` check; a rejection (`422`) or dry run leaves the brain exactly as it was. When the commit goes ahead the evidence is registered and the candidates committed (two snapshots), and the response adds `source` (the canonical block id) and `evidence_duplicate`. Candidates cite the inline evidence as the literal `"$source"`. Decoded evidence over `GBMS_STORE_MAX_EVIDENCE_BYTES` is `413 usage`.

```bash
# Create, register evidence, commit what a model proposed from it, push.
curl -X POST $GBMS/v1/brains -H "Authorization: Bearer $T" \
     -d '{"reference": "brains/handbook"}' -H 'content-type: application/json'
curl -X POST $GBMS/v1/brains/brains/handbook/evidence -H "Authorization: Bearer $T" \
     -F file=@policy.txt -F media_type=text/plain
curl -X POST $GBMS/v1/brains/brains/handbook/commits -H "Authorization: Bearer $T" \
     -H 'content-type: application/json' -d '{
       "source": "sha256:…", "producer": {"kind": "model", "id": "openai/gpt-4.1"},
       "candidates": [{"memory_type": "semantic", "evidence": ["sha256:…"],
                       "payload": {"kind": "fact", "label": "vacation",
                                   "statement": "Employees get 25 vacation days."}}]}'
curl -X POST $GBMS/v1/brains/brains/handbook/branches/main/push -H "Authorization: Bearer $T"
```

## MCP endpoints

The MCP server is mounted under the configured `GBMS_MCP_PATH` (default `/mcp`). Each brain gets its own URL:

```text
/mcp/brains/my-brain:latest
/mcp/registry.example.com/acme/handbook@sha256:abc123...
```

The transport is Streamable HTTP. Each endpoint exposes:

- `brain_search` — semantic/procedural search over the brain.
- `brain_read_block` — fetch and return a single block by id.
- Prompts: `answer_from_brain`, `explore_brain`.

Brain metadata (digest, modules, signature status, MCP URLs) is not an MCP tool;
read it from `GET /v1/brains/{ref}`.

## Brain references

Short references are expanded with the configured defaults:

| Input | Expanded (defaults) |
|-------|---------------------|
| `brains/my-brain:latest` | `registry.alquimia.ai/brains/my-brain:latest` |
| `my-brain` | `registry.alquimia.ai/brains/my-brain:latest` |
| `registry.example.com/acme/handbook:main` | unchanged |

Digest pinning is supported: `brains/my-brain@sha256:...`.
