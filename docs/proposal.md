# Proposal: Gaussia Brains MCP Server

**Epic goal:** Serve Boltzmann Brains to agents as **headless, signed, MCP-served knowledge**.
- Someone publishes a brain to an OCI registry, signs it keyless with their OIDC identity, and indexes it.
- A user browses the index, sees who signed each brain, and picks one for an agent.
- The agent connects to that brain's MCP endpoint and reads it. Nothing else.

The service replaces the brain read path in `alquimia-core` and `alquimia-runtime`. It has its own repository, release lifecycle and configuration, and imports no Alquimia package.

**Target repository:** `https://github.com/alquimia-ai/gaussia-brains-mcp-server`

**Status:** Phase 1 implemented and tested (§13). Per-brain MCP endpoints, REST discovery/management, short brain references, and namespace-index publishing are all working against `registry.alquimia.ai/brains`.

---

## 0. Decisions

| # | Decision |
|---|----------|
| D1 | **Headless brains.** The server holds no registration, binding or per-brain configuration. A brain exists because it was published. What the server will serve is decided by the source allow-list (§5.4) and the signature policy (§6). |
| D2 | **Agents connect to one brain.** Each brain has its own MCP endpoint. Its tools search and read *that* brain only. MCP exposes no listing and no modification. |
| D3 | **The server teaches the agent.** MCP `instructions`, tool descriptions and prompts are generated from the brain's manifest, so a bare MCP connection is enough to use a brain well (§4.2). |
| D4 | **Keyless signatures, feature-flagged.** Publishers sign with Sigstore (OIDC identity). The server verifies and reports *who* signed. `GBMS_SIGNATURE_POLICY` (`off`/`warn`/`required`) decides whether only allow-listed signers are served (§6). |
| D5 | **Index-managed discovery.** Brains are listed from the registry's namespace index, the same mechanism agentspaces (assistants) use, extended with server-verified signer identities (§7). |
| D6 | **Coarse authorization.** Any valid token carrying `brains:read` may read any servable brain, in full. No tenants, tiers, per-module rules or approvals (§5.3). |
| D7 | **Pin or follow, user's choice.** An agent connects by digest (`@sha256:…`, immutable) or by tag (`:main`, follows new publishes). Both are offered when a brain is picked. |
| D8 | **OAuth 2.0 resource server, open standards, no SPIFFE dependency** (§5). |
| D9 | **One listener.** REST (`/v1`), the RFC 9728 metadata and MCP (`/mcp/...`, Streamable HTTP) share one port and one OAuth resource identifier. stdio serves a single configured brain. |

---

## 1. Flow

```text
 Publisher                     OCI registry                       User / platform UI
 ─────────                     ────────────                       ──────────────────
 boltzmann push  ───────────►  <ns>/handbook:main  (brain)
 cosign sign (keyless,  ────►  signature referrer (Fulcio cert:
   OIDC identity)                issuer + identity, Rekor entry)
 index attach   ────────────►  <ns>/index:v1  ◄── entry (kind=brain)
                                      │
                                      ▼
                       gaussia-brains-mcp-server
                       GET /v1/brains ──────────────────────────►  list: description,
                         (index + verified signers)                 signed by, trusted?,
                                                                    MCP URLs (pin / follow)
                                                                          │ user picks
                                                                          ▼
 Agent  ── MCP (Bearer JWT) ──►  /mcp/<ns>/handbook@sha256:…   ◄── agent config stores URL
          initialize → instructions about *this* brain
          tools: brain_search · brain_read_block · brain_resolve
```

---

## 2. Scope

**In scope:**
- MCP serving of brains (stdio and Streamable HTTP).
- Generated agent guidance.
- In-memory brain cache.
- Signature verification and the signer allow-list.
- Index-based discovery over REST.
- JWT auth.

**Out of scope:**
- **Pushing and signing artifacts:** building a brain and signing it with cosign still belongs to publisher tooling (`pyboltzmann` / `alquimia` CLI, CI workflows).
- **Picking brains for agents:** storing which brain an agent uses belongs to the agent platform's config. This service hands out the URLs.
- **Writes through MCP.**

**In scope (management):**
- `POST /v1/brains/{brain_ref}/publish` attaches a `kind=brain` entry to the namespace index for an artifact that already exists in the registry.
- `POST /v1/brains/{brain_ref}/unpublish` removes the index entry.
- `POST /v1/brains/reindex` rebuilds entries for a namespace or a list of refs.

These endpoints require the `brains:publish` scope and use the same short-reference expansion as the read path.

**Decoupling rules:**
- No `alquimia.*` imports; CI will enforce this with an import-linter contract.
- No Alquimia vocabulary in the API, config or claims.
- The namespace index format is documented here as a spec (§7.2) rather than imported.

---

## 3. Architecture

```text
src/gaussia_brains_mcp_server/
├── config.py       # GBMS_* settings, TrustedIssuer, TrustedSigner, sources, short-ref expansion [done]
├── auth/           # JWT resource server: verifier, JWKS, scopes, HTTP deps, MCP adapter [done]
├── signatures.py   # cosign keyless/key verification, signer allow-list, cache      [done]
├── repo.py         # short brain-reference parsing and expansion                     [done]
├── client.py       # RegistryClient over oras; digest refs; signature gate; index writes [done]
├── cache.py        # BrainCache: single-flight, revalidation, pinning, sources      [done]
├── store.py        # build_brain, reader_actor, IndexProvider hook                   [done]
├── errors.py       # stable error codes                                              [done]
├── catalog.py      # namespace-index reader, signer enrichment, publish/unpublish/reindex [done]
├── guidance.py     # MCP instructions / tool descriptions / prompts from a manifest  [done]
├── mcp_server.py   # per-brain MCP endpoints and tools                               [done]
├── api/            # REST /v1                                                         [done]
├── indices/        # ported search engines + embedding client                         [todo]
└── main.py         # app: REST + MCP on one listener; stdio mode                      [done]
```

---

## 4. MCP interface

### 4.1 Endpoints

| Transport | Brain selection |
|-----------|-----------------|
| Streamable HTTP | The path names the brain. `/mcp/<registry>/<repo>@sha256:<digest>` is **pinned**. `/mcp/<registry>/<repo>:<tag>` **follows** the tag. The reference must be inside an allowed source (§5.4). |
| stdio | `GBMS_STDIO_BRAIN=<reference>`. The caller identity comes from `GBMS_STDIO_BEARER_TOKEN`, validated at startup like any bearer token. Without one, it starts only in insecure dev mode. |

A session is bound to one brain for its whole life. No tool takes a brain argument and no tool lists brains.

### 4.2 Generated guidance (D3)

All guidance is built from the brain's manifest and the signature report, and regenerated when a followed tag moves:

- **`instructions`** (MCP initialize result). States:
  - what the brain is: title and description from the manifest annotations;
  - which memory modules it holds, and what each means (canonical = source documents, semantic = facts/concepts, episodic = events, procedural = how-tos, provenance = origins);
  - who signed it and whether this server trusts that signer;
  - the snapshot digest.

  It also tells the agent how to work with it:
  - query with a few specific keywords in the language of the sources;
  - results are verified blocks, not prose: read them and draw your own conclusion;
  - cite block ids;
  - use `expand_depth` to follow relations;
  - use `brain_read_block` to open a cited source;
  - treat an empty result as "the brain holds nothing on this", not as an error.
- **Tool descriptions**:
  - name the brain and list its actual modules in the `memory_types` enum;
  - state limits (`limit`, `expand_depth`).
- **Prompts**:
  - `answer_from_brain(question)`: a template that makes the model search, read and cite before answering.
  - `explore_brain(topic)`: a template for surveying what the brain holds on a topic.

The rules are carried over from the runtime's `_brain_search_tool` description, which already encodes them, so behavior is consistent across agents and MCP clients.

### 4.3 Tools

| Tool | Input | Output |
|------|-------|--------|
| `brain_search` | `query`, `memory_types?`, `limit=10`, `expand_depth=0` | Verified blocks: `block_id`, `memory_type`, `content` (text), `score`, `source`, `locator`, `resolvable`. Unverified blocks are dropped; canonical blocks render their source passage. |
| `brain_read_block` | `block_id` | The block (`Brain.resolve`) with provenance. |
| `brain_resolve` | n/a | Digest, tag, modules, signature report, authorship state. |

All three require `brains:read`.

### 4.4 Errors

A stable `code` and HTTP status, or an MCP tool error `{code, message}`:

| Status | Codes |
|--------|-------|
| 400 | `MEMORY_TYPE_INVALID`, `QUERY_INVALID` |
| 401 | `INVALID_TOKEN` |
| 403 | `SOURCE_NOT_ALLOWED`, `UNTRUSTED_SIGNATURE` |
| 404 | `REFERENCE_NOT_FOUND`, `BLOCK_NOT_FOUND` |
| 409 | `AUTHENTICITY_REJECTED`, `ROLLBACK_REFUSED` |
| 410 | `BLOCK_TOMBSTONED` |
| 422 | `MANIFEST_REJECTED` |
| 500 | `INTERNAL` |
| 502 | `REGISTRY_ERROR` |
| 503 | `BRAIN_UNAVAILABLE` |

Codes are part of the semver contract.

---

## 5. Authentication and authorization

### 5.1 Resource server

The service validates JWT access tokens. It never issues them.

**Issuer and algorithm:**
- The issuer must exactly match a configured `TrustedIssuer`. Nothing is fetched for unknown issuers.
- Only asymmetric algorithms, from a per-issuer allow-list; `none` and `HS*` can't be configured. If a JWK pins an `alg`, the token must use it.
- Optionally, `typ=at+jwt` is required (RFC 9068).

**Keys:**
- Discovered via OIDC Discovery or RFC 8414. The metadata must name the issuer.
- Cached, refetched when an unknown `kid` appears (rate-limited), and the held keys are kept if a refresh fails.

**Claims:**
- `exp`, `iss` and `aud` are required.
- `aud` must be `GBMS_AUTH_RESOURCE` or the issuer's configured audiences (RFC 8707).

**Errors:**
- 401 with `WWW-Authenticate: Bearer resource_metadata="…"` (RFC 6750, RFC 9728).
- 403 `insufficient_scope` with a `scope=` hint.

**Discovery:** `/.well-known/oauth-protected-resource` lists every trusted issuer, so MCP clients can find where to get a token.

### 5.2 Claims

The per-issuer mapping reads `sub`, scopes (`scope`/`scp`), roles (dotted paths, e.g. `realm_access.roles`), `client_id`/`azp`, and RFC 8693 `act.sub`. `role_scopes` turns roles into scopes for IdPs that only issue roles.

### 5.3 Authorization (D6)

| Scope | Allows |
|-------|--------|
| `brains:read` | Connect to any servable brain over MCP and use its tools on every module. |
| `brains:catalog` | `GET /v1/brains` and `GET /v1/brains/{brain_ref}` (discovery). |
| `brains:publish` | `POST /v1/brains/reindex`, `POST /v1/brains/{brain_ref}/publish`, `POST /v1/brains/{brain_ref}/unpublish`. |

Nothing finer-grained exists. A brain is servable when its reference is in an allowed source (§5.4) and it passes the signature policy (§6).

Restricting *which* agents use *which* brains is the platform's job, when it configures an agent with a URL. This service doesn't enforce it. If that's ever needed, per-brain audiences (RFC 8707 resource = the brain's MCP URL) add it without a schema change.

### 5.4 Source allow-list

Headless means callers name references. To keep the server from pulling from any registry on the internet, `GBMS_ALLOWED_SOURCES` lists the `registry/namespace` prefixes it may serve. It defaults to `GBMS_CATALOG_NAMESPACES`, which in turn defaults to `{GBMS_REGISTRY_DEFAULT}/{GBMS_REGISTRY_NAMESPACE_BRAINS}` (`registry.alquimia.ai/brains`). When no defaults can be inferred, every brain is refused and a startup problem is reported (fail closed).

### 5.5 Getting tokens

The code is identical in every case; only `GBMS_AUTH_ISSUERS` changes.

| Caller | Recipe |
|--------|--------|
| Agent runtime | Client credentials or RFC 8693 token exchange, with `resource`/`aud` = `GBMS_AUTH_RESOURCE` and scope `brains:read`. |
| In-cluster workload | Kubernetes projected service-account token with that audience; trust the cluster's OIDC issuer. |
| SPIRE estate | Trust the SPIRE OIDC Discovery Provider as an issuer. JWT-SVIDs validate like any JWT. |
| Interactive MCP client | Reads the RFC 9728 metadata, then authorization code + PKCE at the IdP. |
| Local dev | A local IdP container, **or** `GBMS_AUTH_ENABLED=false` + `GBMS_ALLOW_INSECURE_DEV=true` (both required). |

---

## 6. Signatures (D4)

There are two independent layers. Both are configurable:

| Layer | Question | Mechanism | Setting |
|-------|----------|-----------|---------|
| **Publisher** | Who published this artifact, and do we trust them? | cosign / Sigstore keyless: Fulcio certificate bound to an OIDC identity, Rekor transparency log. A key (path/KMS) is supported as an alternative. | `GBMS_SIGNATURE_POLICY`, `GBMS_TRUSTED_SIGNERS`, `GBMS_COSIGN_KEY` |
| **Authorship** | Was each snapshot signed by a key in the brain's own trust root? | pyboltzmann governance | `GBMS_VERIFY_UNSIGNED`, `GBMS_REQUIRED_SIGNATURES` |

### 6.1 Publisher verification (`signatures.py`, implemented)

1. Always verify `repo@sha256:<digest of the bytes served>`, never a tag, so a tag moved between check and pull can't swap content.
2. `cosign verify --certificate-identity-regexp .+ --certificate-oidc-issuer-regexp .+ -o json`. cosign verifies the certificate chain, the transparency log and the digest, then reports every verified identity.
3. Each verified `(issuer, subject)` pair is matched against `GBMS_TRUSTED_SIGNERS`:
   - `issuer` is exact;
   - the subject is either `subject` (exact) or `subject_regex` (full match).

   Matching pairs here prevents trusting identity A as issued by issuer B.
4. Reports are cached per digest (TTL 10 min, since signatures can be *added* beside a digest).

**Policy:**

| Policy | Behavior |
|--------|----------|
| `off` | No check. |
| `warn` | Served; untrusted ones are logged. |
| `required` | Only trusted signers' brains are served (`UNTRUSTED_SIGNATURE`). |

`warn` and `required` refuse to start without signers or a key.

**Example:**

```bash
GBMS_SIGNATURE_POLICY=required
GBMS_TRUSTED_SIGNERS='[
  {"issuer": "https://token.actions.githubusercontent.com",
   "subject_regex": "https://github\\.com/acme/knowledge/\\.github/workflows/publish\\.yml@refs/heads/main"},
  {"issuer": "https://accounts.google.com", "subject": "alice@acme.com"}
]'
```

**Verified against real Sigstore:** a public keyless-signed image (`cgr.dev/chainguard/static`) is trusted with a matching GitHub-workflow rule and refused with a non-matching one.

**Private Sigstore deployments** (own Fulcio/Rekor) are configured through cosign's standard `SIGSTORE_*`/TUF environment variables, passed through to the process.

### 6.2 Interplay with authorship

Keyless publisher signatures make pyboltzmann's trust root optional for many publishers. Today `GBMS_VERIFY_UNSIGNED=refuse` would refuse every ungoverned brain *even when its publisher signature is trusted*. See §14 Q1.

---

## 7. Discovery (D5)

### 7.1 REST

`GET /v1/brains?namespace=<registry/ns>&cursor=&limit=` (scope `brains:catalog`) reads each namespace's index and returns entries. Brain-reference parameters accept short forms such as `brains/my-brain:latest`, which are expanded using `GBMS_REGISTRY_DEFAULT` and `GBMS_REGISTRY_NAMESPACE_BRAINS`.

```json
{
  "reference": "registry.example.com/acme/handbook",
  "tag": "main",
  "digest": "sha256:…",
  "description": "ACME employee handbook",
  "published_at": "2026-10-30T12:00:00Z",
  "claimed_author": "knowledge-team",
  "signature": {
    "checked": true,
    "trusted": true,
    "signers": [{"method": "keyless", "issuer": "https://token.actions.githubusercontent.com",
                 "subject": "https://github.com/acme/knowledge/.github/workflows/publish.yml@refs/heads/main"}]
  },
  "mcp": {
    "pinned": "https://brains.example.com/mcp/registry.example.com/acme/handbook@sha256:…",
    "follow": "https://brains.example.com/mcp/registry.example.com/acme/handbook:main"
  }
}
```

- `signature` is **verified by this server** on the entry's digest when the policy is not `off`. `claimed_author` is only what the index entry says.
- `GET /v1/brains/{brain_ref}` returns manifest-level detail for one brain; the legacy query-param endpoint `GET /v1/brains/manifest?ref=` still works.
- `POST /v1/brains/{brain_ref}/publish`, `POST /v1/brains/{brain_ref}/unpublish`, and `POST /v1/brains/reindex` manage the namespace index (scope `brains:publish`).
- Signature checks run lazily per page and use the digest cache, so listing stays fast.

### 7.2 Namespace index (format)

The same layout agentspaces use today. Publisher tooling (CLI/CI) is expected to write it; the service also writes it through the `brains:publish` management endpoints:
- **Anchor:** `<registry>/<namespace>/index:v1`, artifact type `application/vnd.alquimia.registry.index.v1`.
- **Entries:** one per `repo:tag`, attached as **OCI referrers** of the anchor. Annotations `ai.alquimia.registry.index.{schema,version,repo,tag,digest,kind,description,author,published-at}`. This service reads `kind=brain` only.

**Rules:**
- An entry may only advertise artifacts in its own namespace; others are ignored, since anyone with push access can write any annotation.
- The advertised digest must still resolve.
- The registry must support the referrers API.

Renaming the annotation keys to a vendor-neutral prefix is §14 Q3.

---

## 8. Brain cache (`cache.py`, implemented)

- **Key:** `(reference, tag-or-digest)`. Brains are pulled whole; there is no caller-dependent narrowing.
- **Single-flight:** the entry being opened is never its own eviction victim.
- **Revalidation:**
  - Followed tags revalidate after `GBMS_CACHE_TTL_SECONDS` by resolving the manifest only.
  - They re-pull when the digest moves, or when new signatures appear on a governed brain not yet authorized.
  - **Pinned digests never revalidate.**
- **Fallbacks:**
  - Registry outages and rejected manifests keep serving the held brain.
  - A failed re-pull keeps it too, unless the authorship policy refuses what is held.
  - A held brain is re-judged under a changed verification policy.
- **Backoff and bound:** exponential backoff on first-open failures; least-recently-checked eviction at `GBMS_CACHE_MAX_BRAINS`.
- **Sources:** references outside allowed sources are refused before anything is fetched.

**Known gap: rollback.** A fresh in-memory pull can't tell that a followed tag moved back to an ancestor. Planned: compare history on a digest move and refuse with `ROLLBACK_REFUSED`.

---

## 9. Search quality

pyboltzmann ships no indices; today the built-in verifying scan answers every query (correct, unranked).

**Parity requires porting core's index engines** behind `IndexProvider`, with query embeddings from an OpenAI-compatible `/v1/embeddings` endpoint:
- settings: `GBMS_EMBEDDINGS_URL`, `GBMS_EMBEDDINGS_MODEL`;
- served by vLLM, TEI, Ollama, LiteLLM or OpenAI.

**Embedding-model mismatch:** vector layers embedded by another model are skipped (already wired via `accepts_vector_index`).

**Conformance test:** a fixture brain in a local Zot registry must produce the same top-k as `alquimia-core` for a fixed query set.

---

## 10. Configuration (main settings)

| Variable | Default | Purpose |
|----------|---------|---------|
| `GBMS_HOST` / `GBMS_PORT` / `GBMS_MCP_PATH` | `0.0.0.0` / `8000` / `/mcp` | One listener |
| `GBMS_MCP_TRANSPORT` | `streamable-http` | or `stdio` |
| `GBMS_STDIO_BRAIN` / `GBMS_STDIO_BEARER_TOKEN` | — | stdio mode |
| `GBMS_REGISTRY_DEFAULT` | `registry.alquimia.ai` | Default registry for short refs |
| `GBMS_REGISTRY_NAMESPACE_BRAINS` | `brains` | Default publishing namespace for short refs |
| `GBMS_CATALOG_NAMESPACES` | `[]` → `{registry}/{namespace}` | Namespaces whose index is listed |
| `GBMS_ALLOWED_SOURCES` | `[]` → catalog namespaces | Where brains may be served from |
| `GBMS_SIGNATURE_POLICY` | `off` | `off` · `warn` · `required` |
| `GBMS_TRUSTED_SIGNERS` | `[]` | Keyless allow-list |
| `GBMS_COSIGN_KEY` | — | Key-based alternative |
| `GBMS_VERIFY_UNSIGNED` / `GBMS_REQUIRED_SIGNATURES` | `refuse` / `1` | Authorship layer |
| `GBMS_AUTH_RESOURCE` / `GBMS_AUTH_ISSUERS` | `http://localhost:8000` / `[]` | OAuth resource, trusted issuers |
| `GBMS_AUTH_ENABLED` / `GBMS_ALLOW_INSECURE_DEV` | `true` / `false` | Dev escape hatch (both needed) |
| `GBMS_CACHE_TTL_SECONDS` / `GBMS_CACHE_MAX_BRAINS` | `300` / `4` | Cache |
| `GBMS_REGISTRY_CONFIG` / `GBMS_REGISTRY_USERNAME` / `_PASSWORD` | — | Registry credentials (password via stdin) |

---

## 11. Lifecycle

- **SemVer.** The contract is the MCP tools and guidance shape, REST `/v1`, error codes, scopes, `GBMS_*` settings and the index format it reads.
- **Image:** `ghcr.io/alquimia-ai/gaussia-brains-mcp-server`, signed keyless from CI (the service holds itself to the rule it enforces), with an SBOM. It bundles pinned `oras` and `cosign`.
- **Compatibility matrix** in the README: gbms × pyboltzmann (exact pin, currently `0.10.1`) × brain artifact schema × MCP spec.

---

## 12. Cutover from `alquimia-core` / `alquimia-runtime`

1. **Release** gbms `0.1.0`: MCP tools, guidance, discovery, auth, signatures.
2. **Publisher side:**
   - brains are pushed, signed keyless (CI workflow identity) and indexed `kind=brain`;
   - a one-off script re-signs and indexes existing brains.
3. **Runtime** (behind a flag, default off):
   - an agent's brain becomes an MCP server URL in its config;
   - `search_mode="brain"` is replaced by the agent's MCP tools;
   - the agent-builder UI uses `GET /v1/brains` to pick a brain (pinned or follow).
4. Flip the flag after staging validation.
5. **Removal PRs:**
   - runtime: `runtime/src/brains.py`, brain registry routes, `BRAIN_*` settings, and the brain branches in `dispatcher.py` and `knowledge/topic.py`; `alquimia-core[boltzmann]` is dropped;
   - core: the brain read path (`boltzmann/cache.py`, read half of `boltzmann/store.py`, `boltzmann/indices/`, `search_mode="brain"`) and the `registry/brain.py` registration and access policy. Authoring stays with the CLI (§14 Q4).

---

## 13. Status

### Done

- [x] `config.py`: `GBMS_` settings; trusted issuers; `TrustedSigner` allow-list; signature policy with startup guard; allowed sources; registry defaults for short refs; single listener; stdio settings; insecure-dev guard; `startup_problems()`.
- [x] `auth/`: JWT verifier, JWKS discovery/cache/rotation, `Principal`, scopes `brains:read`/`brains:catalog`/`brains:publish`, FastAPI dependencies with RFC 6750 challenges, RFC 9728 metadata, MCP SDK adapter.
- [x] `signatures.py`: keyless verification with pair-matched allow-list, key alternative, per-digest cache, off/warn/required gate. Checked against real Sigstore.
- [x] `repo.py`: short brain-reference parsing/expansion; digest pinning.
- [x] `client.py`: digest references; manifest digest from served bytes, checked against pins; signature gate on the digest; oras flags/credentials; namespace-index anchor/entry attach/detach/discover.
- [x] `cache.py`: headless keying, digest pinning, source allow-list, and core's fixes (single-flight, revalidation, outage fallback, signatures, policy re-judging, backoff).
- [x] `catalog.py`: namespace-index reader, signer enrichment, `publish`/`unpublish`/`reindex`.
- [x] `guidance.py`, `mcp_server.py`, `main.py`: per-brain MCP endpoints, generated instructions/tools/prompts, stdio mode, Streamable HTTP dispatch.
- [x] `api/`: REST `/v1/brains` discovery and management endpoints.
- [x] `store.py`, `errors.py`.
- [x] E2E smoke test against `registry.alquimia.ai/brains`.
- [x] Removed: bindings design, per-module authorization (`authz.py`), tenant/tier claims.
- [x] 85 tests, no network required for unit tests.

### Remaining

| Phase | Work | Acceptance |
|-------|------|------------|
| 2 | Index-engine port + embedding client; conformance test | Top-k parity with core |
| 2 | Rollback detection for followed tags | Republished ancestor refused |
| 3 | Dockerfile; CI (ruff, mypy, pytest, import-linter); fixture pipeline in Zot | Green pipeline |
| 3 | Runtime integration behind a flag; agent-builder brain picker; publisher re-sign/index script | Staging agents read brains via MCP |
| 3 | OpenTelemetry, audit log, k8s manifests | Deployed to a test cluster |
| 4 | Removal PRs; `v1.0.0` | No brain read path left in runtime/core |

---

## 14. Open questions

1. **Authorship default.** With keyless publisher signatures as the main trust signal, should `GBMS_VERIFY_UNSIGNED` default to `warn` (accept ungoverned brains whose publisher is trusted) instead of `refuse`?
2. **Signer allow-list scope.** One global list (current), or a list per allowed source/namespace (e.g. namespace `acme/` trusts only ACME's workflows)? A per-namespace list is a backwards-compatible extension of the config.
3. **Index annotation keys.** Keep `ai.alquimia.registry.index.*` for compatibility with agentspace tooling, or move brains to a neutral prefix (e.g. `ai.gaussia.index.*`) and read both during a transition?
4. **Authoring tooling home.** The service now exposes publish/unpublish/reindex REST endpoints. Push and cosign signing still live in publisher tooling (`alquimia` / `pyboltzmann` CLI, CI workflows), which may call these endpoints after signing.
5. **MCP resources.** Also expose canonical documents as MCP resources (`brain://block/<id>`) so clients can open sources natively? Recommendation: after Phase 1.

---

## 15. Risks

| Risk | Mitigation |
|------|------------|
| Any reader can read any servable brain | Source allow-list + `required` signature policy bound what is servable; per-brain audiences remain available if finer control is needed. |
| Sigstore availability (Rekor/TUF) | Per-digest report cache; pinned digests are verified once; private Sigstore supported via cosign env. |
| pyboltzmann alpha churn | Exact pin; conformance gate; thin adapter layer. |
| Search quality before the index port | Don't cut over until the conformance test passes. |
| Followed-tag rollback | Ancestor check (Phase 2); pinning available today. |

---

## 16. Decision log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-11-03 | Standalone service and lifecycle | Decouple brain reads from runtime releases. |
| 2026-11-03 | JWT/OAuth 2.0 resource server, no SPIFFE dependency | Open standards; any IdP; matches the MCP auth spec. |
| 2026-11-03 | Streamable HTTP; one listener | Current MCP transport; one OAuth resource for REST and MCP. |
| 2026-11-03 | Headless brains, no bindings (D1) | Publishing is the only registration; nothing to keep in sync. |
| 2026-11-03 | One brain per MCP connection, read-only, server-generated guidance (D2, D3) | Agents just connect and read; no listing or modification at runtime. |
| 2026-11-03 | Keyless signatures with a feature-flagged signer allow-list (D4) | Trust bound to publisher OIDC identities, visible when choosing a brain. |
| 2026-11-03 | Discovery through the namespace index (D5) | Same rapid discovery as agentspaces. |
| 2026-11-03 | Any valid reader; no per-module rules (D6) | Simplicity; scoping is the platform's choice of URL. |
| 2026-11-03 | Pin or follow, user's choice (D7) | Reproducibility when wanted, freshness when wanted. |
| 2026-11-05 | Short brain references (`brains/name:tag`) and default registry | Usability; matches the public `registry.alquimia.ai/brains` namespace. |
| 2026-11-05 | Management endpoints for publish/unpublish/reindex | Keep publisher tooling (CLI/CI) thin; still require `brains:publish`. |
