"""The lifecycle REST surface end to end, over a real working store.

The registry is pyboltzmann's ``LocalLayoutRegistry`` (OCI layouts on disk),
which implements the same client protocol as the ``oras`` transport, so push,
pull, import and converge run through the SDK's real distribution path.
"""

from __future__ import annotations

import io
import json
import tarfile
import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from boltzmann.distribution.local import LocalLayoutRegistry
from fastapi.testclient import TestClient

from conftest import ISSUER, RESOURCE
from gaussia_brains_mcp_server.auth import scopes
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.config import get_settings
from gaussia_brains_mcp_server.main import create_app

REF = "registry.example.com/acme/handbook"
ANA = Principal(
    subject="0b9e-ana",
    issuer=ISSUER,
    scopes=frozenset({scopes.WRITE, scopes.CATALOG, scopes.READ}),
    claims={"email": "Ana@Example.com", "name": "Ana"},
)
READER = Principal(subject="r", issuer=ISSUER, scopes=frozenset({scopes.CATALOG}))
AUTH = {"Authorization": "Bearer token"}


class StubCatalog:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []

    async def publish(self, ref: str, **kwargs: Any) -> dict[str, Any]:
        self.published.append((ref, kwargs))
        return {"entry_digest": "sha256:" + "e" * 64}


def _configure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, store: bool = True
) -> None:
    get_settings.cache_clear()
    monkeypatch.setenv("GBMS_AUTH_RESOURCE", RESOURCE)
    monkeypatch.setenv("GBMS_AUTH_ISSUERS", json.dumps([{"issuer": ISSUER}]))
    monkeypatch.setenv(
        "GBMS_ALLOWED_SOURCES", json.dumps(["registry.example.com/acme"])
    )
    if store:
        monkeypatch.setenv("GBMS_STORE_ROOT", str(tmp_path / "store"))


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    _configure(monkeypatch, tmp_path)
    app = create_app()
    registry = LocalLayoutRegistry(tmp_path / "registry")
    catalog = StubCatalog()
    with TestClient(app) as client:
        principal = {"value": ANA}
        app.state.token_verifier.verify = AsyncMock(
            side_effect=lambda _t: principal["value"]
        )
        service = app.state.lifecycle
        service._client_factory = lambda *a, **k: registry
        service._catalog_factory = lambda _s: catalog
        yield client, principal, registry, catalog


def _create(client: TestClient, ref: str = REF, **body: Any) -> dict[str, Any]:
    response = client.post("/v1/brains", json={"reference": ref, **body}, headers=AUTH)
    assert response.status_code == 201, response.text
    return response.json()


def _evidence(
    client: TestClient, text: str, ref: str = REF, **form: str
) -> dict[str, Any]:
    response = client.post(
        f"/v1/brains/{ref}/evidence",
        files={"file": ("source.txt", text.encode(), "text/plain")},
        data=form,
        headers=AUTH,
    )
    assert response.status_code == 201, response.text
    return response.json()


def _fact(
    source: str, label: str, statement: str, subject: str | None = None
) -> dict[str, Any]:
    payload = {"kind": "fact", "label": label, "statement": statement}
    if subject:
        payload["subject"] = subject
    return {"memory_type": "semantic", "payload": payload, "evidence": [source]}


def _commit(
    client: TestClient,
    source: str,
    candidates: list[dict[str, Any]],
    ref: str = REF,
    headers: dict[str, str] | None = None,
    **body: Any,
):
    return client.post(
        f"/v1/brains/{ref}/commits",
        json={
            "source": source,
            "candidates": candidates,
            "producer": {"kind": "model", "id": "openai/gpt-4.1"},
            **body,
        },
        headers={**AUTH, **(headers or {})},
    )


def _wait(
    client: TestClient, job: dict[str, Any], timeout: float = 20.0
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        response = client.get(f"/v1/jobs/{job['id']}", headers=AUTH)
        assert response.status_code == 200, response.text
        view = response.json()
        if view["state"] in ("succeeded", "failed", "cancelled"):
            return view
        assert time.monotonic() < deadline, view
        time.sleep(0.05)


# --------------------------------------------------------------------------- #


def test_create_status_and_read_only_replica(env) -> None:
    client, *_ = env
    status = _create(client, title="Handbook", default_tag="stable")
    assert status["reference"] == REF
    assert status["held"] is True
    assert status["default_tag"] == "stable"
    assert status["head"] is None and status["branches"] == []
    assert status["created_by"] == "ana@example.com"

    again = client.post("/v1/brains", json={"reference": REF}, headers=AUTH)
    assert again.status_code == 400
    assert again.json()["code"] == "usage"

    outside = client.post(
        "/v1/brains", json={"reference": "registry.other.com/x/y"}, headers=AUTH
    )
    assert outside.status_code == 403
    assert outside.json()["code"] == "refused_policy"

    bad_tag = client.post(
        "/v1/brains", json={"reference": f"{REF}2", "default_tag": "br.x"}, headers=AUTH
    )
    assert bad_tag.json()["code"] == "usage"

    held = client.get("/v1/brains", params={"held": "true"}, headers=AUTH).json()
    assert [e["reference"] for e in held["entries"]] == [REF]


def test_read_only_replica_refuses_writes(monkeypatch, tmp_path) -> None:
    _configure(monkeypatch, tmp_path, store=False)
    app = create_app()
    with TestClient(app) as client:
        app.state.token_verifier.verify = AsyncMock(return_value=ANA)
        response = client.post("/v1/brains", json={"reference": REF}, headers=AUTH)
    assert response.status_code == 403
    assert response.json()["code"] == "refused_policy"


def test_write_needs_the_write_scope(env) -> None:
    client, principal, *_ = env
    principal["value"] = READER
    response = client.post("/v1/brains", json={"reference": REF}, headers=AUTH)
    assert response.status_code == 403


def test_evidence_commit_browse_search_history(env) -> None:
    client, *_ = env
    _create(client)
    evidence = _evidence(client, "Paris is the capital of France.")
    source = evidence["block_id"]
    assert evidence["duplicate"] is False and evidence["head"]
    assert _evidence(client, "Paris is the capital of France.")["duplicate"] is True

    dry = _commit(
        client,
        source,
        [_fact(source, "paris", "Paris is the capital of France.")],
        dry_run=True,
    )
    assert dry.status_code == 200, dry.text
    assert dry.json()["report"]["clean"] is True
    assert dry.json()["dry_run"] is True

    committed = _commit(
        client, source, [_fact(source, "paris", "Paris is the capital of France.")]
    )
    assert committed.status_code == 201, committed.text
    body = committed.json()
    block_id = body["committed"][0]["block_id"]
    assert body["committed"][0]["memory_type"] == "semantic"
    head = body["head"]
    assert committed.headers["ETag"] == f'"{head}"'

    status = client.get(f"/v1/brains/{REF}", headers=AUTH)
    assert status.status_code == 200
    assert status.headers["ETag"] == f'"{head}"'
    data = status.json()
    assert data["modules"]["semantic"]["block_count"] == 1
    assert data["modules"]["canonical"]["block_count"] == 1
    assert [b["name"] for b in data["branches"]] == ["main"]
    assert data["convergence"] is None

    blocks = client.get(
        f"/v1/brains/{REF}/modules/semantic/blocks", headers=AUTH
    ).json()
    assert blocks["total"] == 1
    row = blocks["blocks"][0]
    assert row["block_id"] == block_id and row["readable"] is True
    assert row["added_at"] and row["superseded_by"] is None

    detail = client.get(f"/v1/brains/{REF}/blocks/{block_id}", headers=AUTH).json()
    assert detail["verifies"] == {
        "membership": True,
        "hash": True,
        "inclusion_proof": True,
    }
    assert detail["evidence"] == [source]
    kinds = {r["record_type"] for r in detail["provenance"]}
    assert {"derivation", "validation"} <= kinds
    derivation = next(
        r for r in detail["provenance"] if r["record_type"] == "derivation"
    )
    assert derivation["actor"]["id"] == "ana@example.com"
    assert derivation["producer"]["id"] == "openai/gpt-4.1"
    assert "Paris" in detail["text"]

    canonical = client.get(f"/v1/brains/{REF}/blocks/{source}", headers=AUTH).json()
    assert canonical["dependents"] == [block_id]
    assert "Paris is the capital" in canonical["text"]

    found = client.post(
        f"/v1/brains/{REF}/search", json={"query": "capital France"}, headers=AUTH
    ).json()
    assert block_id in [r["block_id"] for r in found["results"]]
    assert found["narrowing"]["semantic"] is False
    missing = client.post(
        f"/v1/brains/{REF}/search",
        json={"query": "capital", "memory_types": ["procedural"]},
        headers=AUTH,
    ).json()
    assert missing["narrowing"]["missing_memory_types"] == ["procedural"]

    history = client.get(
        f"/v1/brains/{REF}/history", params={"limit": 1}, headers=AUTH
    ).json()
    assert history["snapshots"][0]["snapshot"] == head
    assert history["next_cursor"]
    rest = client.get(
        f"/v1/brains/{REF}/history",
        params={"cursor": history["next_cursor"]},
        headers=AUTH,
    ).json()
    assert len(rest["snapshots"]) == 1  # the registration snapshot
    assert rest["next_cursor"] is None


def test_if_match_and_rejected_candidates(env) -> None:
    client, *_ = env
    _create(client)
    first = _evidence(client, "Rome is the capital of Italy.")
    stale = first["head"]
    second = _evidence(client, "Madrid is the capital of Spain.")
    source = second["block_id"]

    refused = _commit(
        client,
        source,
        [_fact(source, "madrid", "Madrid.")],
        headers={"If-Match": f'"{stale}"'},
    )
    assert refused.status_code == 409
    assert refused.json()["code"] == "not_fast_forward"
    assert refused.json()["detail"]["head"] == second["head"]

    ok = _commit(
        client,
        source,
        [_fact(source, "madrid", "Madrid.")],
        headers={"If-Match": second["head"]},
    )
    assert ok.status_code == 201, ok.text

    ghost = "sha256:" + "0" * 64
    rejected = _commit(client, source, [_fact(ghost, "x", "cites nothing held")])
    assert rejected.status_code == 422
    body = rejected.json()
    assert body["code"] == "rejected_candidates"
    assert body["detail"]["report"]["results"][0]["status"] == "rejected"

    bad_producer = client.post(
        f"/v1/brains/{REF}/commits",
        json={
            "source": source,
            "candidates": [_fact(source, "y", "y")],
            "producer": {"kind": "model", "id": "GPT 4"},
        },
        headers=AUTH,
    )
    assert bad_producer.json()["code"] == "usage"

    unknown = client.get(f"/v1/brains/{REF}/blocks/sha256:{'1' * 64}", headers=AUTH)
    assert unknown.status_code == 404 and unknown.json()["code"] == "not_found"


def test_branches_and_converge(env) -> None:
    client, *_ = env
    _create(client)
    src = _evidence(client, "Capitals.")["block_id"]
    assert _commit(client, src, [_fact(src, "paris", "Paris.")]).status_code == 201

    created = client.post(
        f"/v1/brains/{REF}/branches", json={"name": "ana/fix"}, headers=AUTH
    )
    assert created.status_code == 201, created.text
    assert created.json()["tag"] == "br.ana.fix"

    assert (
        _commit(
            client, src, [_fact(src, "rome", "Rome.")], branch="ana/fix"
        ).status_code
        == 201
    )
    branches = client.get(f"/v1/brains/{REF}/branches", headers=AUTH).json()["branches"]
    assert [b["name"] for b in branches] == ["main", "ana/fix"]
    main_head = branches[0]["snapshot"]
    fix_head = branches[1]["snapshot"]
    assert main_head != fix_head

    # main is behind ana/fix: a fast-forward, no strategy needed.
    job = client.post(
        f"/v1/brains/{REF}/converge", json={"from": "ana/fix"}, headers=AUTH
    )
    assert job.status_code == 202, job.text
    assert job.headers["Location"].startswith("/v1/jobs/")
    done = _wait(client, job.json())
    assert done["state"] == "succeeded", done
    assert done["result"]["outcome"] == "fast-forward"
    assert client.get(f"/v1/brains/{REF}", headers=AUTH).json()["head"] == fix_head

    # Diverge, then converge: a strategy is required, never defaulted.
    assert _commit(client, src, [_fact(src, "lisbon", "Lisbon.")]).status_code == 201
    assert (
        _commit(
            client, src, [_fact(src, "berlin", "Berlin.")], branch="ana/fix"
        ).status_code
        == 201
    )
    no_strategy = _wait(
        client,
        client.post(
            f"/v1/brains/{REF}/converge", json={"from": "ana/fix"}, headers=AUTH
        ).json(),
    )
    assert no_strategy["state"] == "failed"
    assert no_strategy["error"]["code"] == "usage"
    merged = _wait(
        client,
        client.post(
            f"/v1/brains/{REF}/converge",
            json={"from": "ana/fix", "strategy": "merge", "reason": "join"},
            headers=AUTH,
        ).json(),
    )
    assert merged["state"] == "succeeded", merged
    assert merged["result"]["outcome"] == "reconciled"
    status = client.get(f"/v1/brains/{REF}", headers=AUTH).json()
    assert status["modules"]["semantic"]["block_count"] == 4

    # Branch tags may not collide with main's, and main cannot be deleted.
    assert (
        client.delete(f"/v1/brains/{REF}/branches/main", headers=AUTH).json()["code"]
        == "usage"
    )
    deleted = client.delete(f"/v1/brains/{REF}/branches/ana/fix", headers=AUTH)
    assert deleted.status_code == 200, deleted.text
    gone = client.delete(f"/v1/brains/{REF}/branches/ana/fix", headers=AUTH)
    assert gone.status_code == 404


def test_converge_that_stops_needs_a_human(env) -> None:
    client, *_ = env
    _create(client)
    src = _evidence(client, "Capitals.")["block_id"]
    assert _commit(client, src, [_fact(src, "intro", "Intro.")]).status_code == 201
    client.post(f"/v1/brains/{REF}/branches", json={"name": "other"}, headers=AUTH)
    assert (
        _commit(
            client,
            src,
            [_fact(src, "capital", "Paris is the capital.", subject="france")],
        ).status_code
        == 201
    )
    assert (
        _commit(
            client,
            src,
            [_fact(src, "capital", "Lyon is the capital.", subject="france")],
            branch="other",
        ).status_code
        == 201
    )

    halted = _wait(
        client,
        client.post(
            f"/v1/brains/{REF}/converge",
            json={"from": "other", "strategy": "merge"},
            headers=AUTH,
        ).json(),
    )
    assert halted["state"] == "failed"
    assert halted["error"]["code"] == "needs_human"

    convergence = client.get(f"/v1/brains/{REF}/convergence", headers=AUTH).json()
    assert convergence["open"] is True
    assert len(convergence["unresolved"]) == 1
    assert (
        client.get(f"/v1/brains/{REF}", headers=AUTH).json()["convergence"]["open"]
        is True
    )

    # Writes wait for the convergence to be settled.
    blocked = _commit(client, src, [_fact(src, "x", "x")])
    assert blocked.status_code == 409 and blocked.json()["code"] == "needs_human"
    early = client.post(f"/v1/brains/{REF}/convergence/continue", headers=AUTH)
    assert early.status_code == 409 and early.json()["code"] == "needs_human"

    answered = client.post(
        f"/v1/brains/{REF}/convergence/resolutions",
        json={
            "block": convergence["unresolved"][0],
            "kind": "admit",
            "reason": "both stand",
        },
        headers=AUTH,
    )
    assert answered.status_code == 200, answered.text
    assert answered.json()["resolved"] is True
    finished = client.post(f"/v1/brains/{REF}/convergence/continue", headers=AUTH)
    assert finished.status_code == 200, finished.text
    assert finished.json()["outcome"] == "reconciled"
    assert client.get(f"/v1/brains/{REF}/convergence", headers=AUTH).json() == {
        "open": False
    }
    assert (
        client.post(f"/v1/brains/{REF}/convergence/abort", headers=AUTH).status_code
        == 404
    )


def test_push_pull_import_and_index(env, tmp_path: Path) -> None:
    client, _, registry, catalog = env
    _create(client, description="The handbook")
    src = _evidence(client, "Handbook.")["block_id"]
    assert _commit(client, src, [_fact(src, "hb", "Handbook.")]).status_code == 201

    pushed = _wait(
        client, client.post(f"/v1/brains/{REF}/branches/main/push", headers=AUTH).json()
    )
    assert pushed["state"] == "succeeded", pushed
    assert pushed["result"]["tag"] == "latest"
    assert pushed["result"]["indexed"] is True
    assert registry.tags(REF) == ["latest"]
    assert catalog.published[0][0] == f"{REF}:latest"
    assert catalog.published[0][1]["author"] == "ana@example.com"

    status = client.get(f"/v1/brains/{REF}", headers=AUTH).json()
    assert status["publication"]["up_to_date"] is True
    assert status["branches"][0]["unpublished"] is False

    # A branch pushes to its own tag and is not indexed.
    client.post(f"/v1/brains/{REF}/branches", json={"name": "draft"}, headers=AUTH)
    branch_push = _wait(
        client,
        client.post(f"/v1/brains/{REF}/branches/draft/push", headers=AUTH).json(),
    )
    assert branch_push["result"]["tag"] == "br.draft"
    assert branch_push["result"]["indexed"] is False
    remote = client.get(
        f"/v1/brains/{REF}/branches", params={"remote": "true"}, headers=AUTH
    )
    assert {b["name"] for b in remote.json()["remote"]} == {"main", "draft"}

    # Import the published brain under another name, then pull it again.
    other = "registry.example.com/acme/copy"
    imported = client.post(
        f"/v1/brains/{other}/import", json={"source": f"{REF}:latest"}, headers=AUTH
    )
    assert imported.status_code == 202, imported.text
    done = _wait(client, imported.json())
    assert done["state"] == "succeeded", done
    copy = client.get(f"/v1/brains/{other}", headers=AUTH).json()
    assert copy["head"] == status["head"]
    assert copy["imported_from"] == f"{REF}:latest"
    assert copy["modules"]["semantic"]["block_count"] == 1

    pulled = _wait(client, client.post(f"/v1/brains/{REF}/pull", headers=AUTH).json())
    assert pulled["state"] == "succeeded", pulled
    assert pulled["result"]["snapshot"] == status["head"]

    # Converge a published tag into main (already contained: nothing to do).
    contained = _wait(
        client,
        client.post(
            f"/v1/brains/{REF}/converge", json={"from": f"{REF}:latest"}, headers=AUTH
        ).json(),
    )
    assert contained["state"] == "succeeded", contained
    assert contained["result"]["outcome"] == "up-to-date"
    branches = client.get(f"/v1/brains/{REF}/branches", headers=AUTH).json()["branches"]
    assert not any(b["name"].startswith("converge/") for b in branches)

    missing = _wait(
        client,
        client.post(
            "/v1/brains/registry.example.com/acme/nothing/import", json={}, headers=AUTH
        ).json(),
    )
    assert missing["state"] == "failed"
    assert missing["error"]["code"] == "not_found"
    held = client.get("/v1/brains", params={"held": "true"}, headers=AUTH).json()[
        "entries"
    ]
    assert "registry.example.com/acme/nothing" not in [e["reference"] for e in held]


def test_import_uploaded_layout(env, tmp_path: Path) -> None:
    client, *_ = env
    _create(client)
    src = _evidence(client, "Layout.")["block_id"]
    assert _commit(client, src, [_fact(src, "l", "Layout.")]).status_code == 201
    head = client.get(f"/v1/brains/{REF}", headers=AUTH).json()["head"]

    app = client.app
    layout = app.state.lifecycle.store.get(REF).layout
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        tar.add(layout, arcname="my-brain")
    other = "registry.example.com/acme/uploaded"
    response = client.post(
        f"/v1/brains/{other}/import",
        files={"layout": ("brain.tar.gz", buffer.getvalue(), "application/gzip")},
        data={"title": "Uploaded"},
        headers=AUTH,
    )
    assert response.status_code == 202, response.text
    done = _wait(client, response.json())
    assert done["state"] == "succeeded", done
    assert done["result"]["snapshot"] == head
    status = client.get(f"/v1/brains/{other}", headers=AUTH).json()
    assert status["head"] == head and status["title"] == "Uploaded"

    junk = client.post(
        "/v1/brains/registry.example.com/acme/junk/import",
        files={"layout": ("x.tar", b"not a tar", "application/x-tar")},
        headers=AUTH,
    )
    failed = _wait(client, junk.json())
    assert failed["state"] == "failed" and failed["error"]["code"] == "usage"


def test_jobs_cannot_be_cancelled_once_finished(env) -> None:
    client, *_ = env
    assert client.get("/v1/jobs/job_nope", headers=AUTH).json()["code"] == "not_found"
    _create(client)
    job = _wait(client, client.post(f"/v1/brains/{REF}/pull", headers=AUTH).json())
    assert job["state"] == "failed" and job["error"]["code"] == "not_found"
    refused = client.post(f"/v1/jobs/{job['id']}/cancel", headers=AUTH)
    assert refused.status_code == 400 and refused.json()["code"] == "usage"
    listed = client.get("/v1/jobs", params={"brain": REF}, headers=AUTH).json()["jobs"]
    assert [j["id"] for j in listed] == [job["id"]]
