"""Who may see jobs, write brains and settle convergences; the replica header.

Runs the REST surface over a real working store, switching the caller between
the brain's owner (Ana), another writer (Bob) and an administrator.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from conftest import ISSUER
from gaussia_brains_mcp_server.auth import scopes
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.main import create_app
from test_lifecycle import (
    ANA,
    AUTH,
    REF,
    _commit,
    _configure,
    _create,
    _evidence,
    _fact,
    _wait,
    env,  # noqa: F401 - the fixture
)

BOB = Principal(
    subject="bob",
    issuer=ISSUER,
    scopes=frozenset({scopes.WRITE, scopes.CATALOG, scopes.READ}),
    claims={"email": "bob@example.com"},
)
ADMIN = Principal(
    subject="root",
    issuer=ISSUER,
    scopes=frozenset({scopes.WRITE, scopes.CATALOG, scopes.ADMIN}),
    claims={"email": "root@example.com"},
)
WRITE_ONLY = Principal(
    subject="w",
    issuer=ISSUER,
    scopes=frozenset({scopes.WRITE}),
    claims={"email": "bob@example.com"},
)


def _seeded(client: TestClient, ref: str = REF) -> str:
    _create(client, ref)
    src = _evidence(client, "Capitals.", ref=ref)["block_id"]
    assert (
        _commit(client, src, [_fact(src, "intro", "Intro.")], ref=ref).status_code
        == 201
    )
    return src


# --------------------------------------------------------------------------- #
# S1: jobs are their actor's
# --------------------------------------------------------------------------- #


def test_jobs_are_visible_only_to_their_actor(env) -> None:  # noqa: F811
    client, principal, *_ = env
    _seeded(client)
    job = _wait(client, client.post(f"/v1/brains/{REF}/pull", headers=AUTH).json())
    assert job["actor"] == "ana@example.com"

    principal["value"] = BOB
    hidden = client.get(f"/v1/jobs/{job['id']}", headers=AUTH)
    assert hidden.status_code == 404 and hidden.json()["code"] == "not_found"
    cancel = client.post(f"/v1/jobs/{job['id']}/cancel", headers=AUTH)
    assert cancel.status_code == 404 and cancel.json()["code"] == "not_found"
    assert client.get("/v1/jobs", headers=AUTH).json() == {"jobs": []}

    principal["value"] = ADMIN
    assert client.get(f"/v1/jobs/{job['id']}", headers=AUTH).status_code == 200
    assert [j["id"] for j in client.get("/v1/jobs", headers=AUTH).json()["jobs"]] == [
        job["id"]
    ]

    principal["value"] = ANA
    assert [j["id"] for j in client.get("/v1/jobs", headers=AUTH).json()["jobs"]] == [
        job["id"]
    ]


# --------------------------------------------------------------------------- #
# S2: per-brain write access
# --------------------------------------------------------------------------- #


def test_only_owners_and_writers_write(env) -> None:  # noqa: F811
    client, principal, *_ = env
    src = _seeded(client)

    principal["value"] = BOB
    refused = _commit(client, src, [_fact(src, "bob", "Bob.")])
    assert refused.status_code == 403
    body = refused.json()
    assert body["code"] == "refused_policy"
    assert body["detail"] == {"access": "owners", "owners": ["ana@example.com"]}
    assert "ana@example.com" in body["message"] and "/writers" in body["message"]
    for response in (
        client.post(
            f"/v1/brains/{REF}/evidence",
            files={"file": ("a.txt", b"x", "text/plain")},
            headers=AUTH,
        ),
        client.post(f"/v1/brains/{REF}/branches", json={"name": "b"}, headers=AUTH),
        client.post(f"/v1/brains/{REF}/branches/main/push", headers=AUTH),
        client.post(f"/v1/brains/{REF}/pull", headers=AUTH),
        client.post(f"/v1/brains/{REF}/converge", json={"from": "x"}, headers=AUTH),
        client.post(f"/v1/brains/{REF}/import", json={}, headers=AUTH),
    ):
        assert response.status_code == 403, response.text
        assert response.json()["code"] == "refused_policy"
    # Bob may not grant himself access either.
    grant = client.put(
        f"/v1/brains/{REF}/writers", json={"writers": ["bob@example.com"]}, headers=AUTH
    )
    assert grant.status_code == 403 and grant.json()["code"] == "refused_policy"
    status = client.get(f"/v1/brains/{REF}", headers=AUTH).json()
    assert status["owner"] == "ana@example.com"
    assert status["writers"] == [] and status["writable"] is False

    principal["value"] = ANA
    bad = client.put(
        f"/v1/brains/{REF}/writers", json={"writers": ["Not An Id"]}, headers=AUTH
    )
    assert bad.status_code == 400 and bad.json()["code"] == "usage"
    granted = client.put(
        f"/v1/brains/{REF}/writers",
        json={"writers": ["Bob@Example.com", "bob@example.com"]},
        headers=AUTH,
    )
    assert granted.status_code == 200, granted.text
    assert granted.json() == {
        "reference": REF,
        "owner": "ana@example.com",
        "writers": ["bob@example.com"],
        "access": "owners",
    }

    principal["value"] = BOB
    assert _commit(client, src, [_fact(src, "bob", "Bob.")]).status_code == 201
    assert client.get(f"/v1/brains/{REF}", headers=AUTH).json()["writable"] is True
    viewed = client.get(f"/v1/brains/{REF}/writers", headers=AUTH)
    assert viewed.status_code == 200 and viewed.json()["writers"] == ["bob@example.com"]
    # A writer is not an owner: Bob still cannot change the list.
    assert (
        client.put(
            f"/v1/brains/{REF}/writers", json={"writers": []}, headers=AUTH
        ).status_code
        == 403
    )

    # Persisted with the meta.
    meta = client.app.state.lifecycle.store.get(REF)
    on_disk = json.loads((meta.directory / "brain.json").read_text())
    assert on_disk["writers"] == ["bob@example.com"]


def test_admin_writes_any_brain_and_legacy_brains_need_one(env) -> None:  # noqa: F811
    client, principal, *_ = env
    src = _seeded(client)
    handle = client.app.state.lifecycle.store.get(REF)

    def legacy(meta: Any) -> None:
        meta.created_by = ""

    handle.update_meta(legacy)
    principal["value"] = ANA
    refused = _commit(client, src, [_fact(src, "a", "A.")])
    assert refused.status_code == 403
    assert refused.json()["detail"] == {"access": "owners", "owners": []}
    assert "administrator" in refused.json()["message"]
    assert (
        client.put(
            f"/v1/brains/{REF}/writers",
            json={"writers": ["ana@example.com"]},
            headers=AUTH,
        ).status_code
        == 403
    )

    principal["value"] = ADMIN
    assert _commit(client, src, [_fact(src, "root", "Root.")]).status_code == 201
    assert (
        client.put(
            f"/v1/brains/{REF}/writers",
            json={"writers": ["ana@example.com"]},
            headers=AUTH,
        ).status_code
        == 200
    )
    principal["value"] = ANA
    assert _commit(client, src, [_fact(src, "a", "A.")]).status_code == 201


def test_open_access_keeps_any_writer(monkeypatch, tmp_path: Path) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setenv("GBMS_BRAIN_ACCESS", "open")
    app = create_app()
    with TestClient(app) as client:
        principal = {"value": ANA}
        app.state.token_verifier.verify = AsyncMock(
            side_effect=lambda _t: principal["value"]
        )
        src = _seeded(client)
        principal["value"] = BOB
        assert _commit(client, src, [_fact(src, "bob", "Bob.")]).status_code == 201
        assert client.get(f"/v1/brains/{REF}/writers", headers=AUTH).json()[
            "access"
        ] == ("open")
        held = client.get("/v1/brains", params={"held": "true"}, headers=AUTH).json()
        assert [(e["reference"], e["writable"]) for e in held["entries"]] == [
            (REF, True)
        ]


def test_held_listing_is_filtered_and_paginated(env) -> None:  # noqa: F811
    client, principal, *_ = env
    refs = [f"registry.example.com/acme/b{i}" for i in range(3)]
    for ref in refs:
        _create(client, ref)
    principal["value"] = BOB
    _create(client, "registry.example.com/acme/bobs")

    held = client.get("/v1/brains", params={"held": "true"}, headers=AUTH).json()
    assert [e["reference"] for e in held["entries"]] == [
        "registry.example.com/acme/bobs"
    ]
    assert held["entries"][0]["writable"] is True and held["next_cursor"] is None

    # brains:write alone may list held brains (not the registry catalog).
    principal["value"] = WRITE_ONLY
    only_write = client.get("/v1/brains", params={"held": "true"}, headers=AUTH)
    assert only_write.status_code == 200, only_write.text
    assert client.get("/v1/brains", headers=AUTH).status_code == 403

    principal["value"] = ANA
    seen: list[str] = []
    cursor = None
    while True:
        params: dict[str, Any] = {"held": "true", "limit": 2}
        if cursor:
            params["cursor"] = cursor
        page = client.get("/v1/brains", params=params, headers=AUTH).json()
        assert len(page["entries"]) <= 2
        assert all(e["writable"] for e in page["entries"])
        seen += [e["reference"] for e in page["entries"]]
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert seen == refs

    principal["value"] = ADMIN
    every = client.get("/v1/brains", params={"held": "true"}, headers=AUTH).json()
    assert len(every["entries"]) == 4


def test_writers_routes_are_not_shadowed(env) -> None:  # noqa: F811
    client, *_ = env
    _create(client)
    got = client.get(f"/v1/brains/{REF}/writers", headers=AUTH)
    assert got.status_code == 200
    assert set(got.json()) == {"reference", "owner", "writers", "access"}
    short = client.get("/v1/brains/acme/handbook/writers", headers=AUTH)
    assert short.status_code == 404  # brains/acme/handbook is another (unheld) brain
    reserved = client.post(
        "/v1/brains",
        json={"reference": "registry.example.com/acme/writers"},
        headers=AUTH,
    )
    assert reserved.status_code == 400 and reserved.json()["code"] == "usage"


def test_import_into_a_held_brain_needs_access(env) -> None:  # noqa: F811
    client, principal, *_ = env
    _create(client)
    principal["value"] = BOB
    refused = client.post(f"/v1/brains/{REF}/import", json={}, headers=AUTH)
    assert refused.status_code == 403 and refused.json()["code"] == "refused_policy"
    principal["value"] = ANA
    exists = client.post(f"/v1/brains/{REF}/import", json={}, headers=AUTH)
    assert exists.status_code == 400 and exists.json()["code"] == "usage"


def test_import_records_the_importer_as_owner(env) -> None:  # noqa: F811
    client, principal, *_ = env
    _seeded(client)
    assert (
        _wait(
            client,
            client.post(f"/v1/brains/{REF}/branches/main/push", headers=AUTH).json(),
        )["state"]
        == "succeeded"
    )
    principal["value"] = BOB
    other = "registry.example.com/acme/bobcopy"
    done = _wait(
        client,
        client.post(
            f"/v1/brains/{other}/import", json={"source": f"{REF}:latest"}, headers=AUTH
        ).json(),
    )
    assert done["state"] == "succeeded", done
    status = client.get(f"/v1/brains/{other}", headers=AUTH).json()
    assert status["owner"] == "bob@example.com" and status["writable"] is True


# --------------------------------------------------------------------------- #
# S4: one convergence per brain, settled by its actor, the owner or an admin
# --------------------------------------------------------------------------- #


def _open_convergence(client: TestClient, src: str) -> dict[str, Any]:
    client.post(f"/v1/brains/{REF}/branches", json={"name": "other"}, headers=AUTH)
    client.post(f"/v1/brains/{REF}/branches", json={"name": "spare"}, headers=AUTH)
    assert (
        _commit(
            client, src, [_fact(src, "capital", "Paris.", subject="france")]
        ).status_code
        == 201
    )
    assert (
        _commit(
            client,
            src,
            [_fact(src, "capital", "Lyon.", subject="france")],
            branch="other",
        ).status_code
        == 201
    )
    halted = _wait(
        client,
        client.post(
            f"/v1/brains/{REF}/converge",
            json={"from": "other", "strategy": "merge", "reason": "join"},
            headers=AUTH,
        ).json(),
    )
    assert halted["state"] == "failed" and halted["error"]["code"] == "needs_human"
    detail = halted["error"]["detail"]["convergence"]
    assert detail["branch"] == "main" and detail["actor"] == "ana@example.com"
    assert detail["theirs"] == "other" and "unresolved" in detail
    return client.get(f"/v1/brains/{REF}/convergence", headers=AUTH).json()


def test_open_convergence_blocks_every_branch(env) -> None:  # noqa: F811
    client, principal, *_ = env
    src = _seeded(client)
    view = _open_convergence(client, src)
    assert view["open"] is True
    assert view["actor"] == "ana@example.com" and view["theirs"] == "other"
    assert view["strategy"] == "merge" and view["reason"] == "join"
    assert view["started_at"]

    expected = {"branch": "main", "actor": "ana@example.com", "theirs": "other"}
    for response in (
        _commit(client, src, [_fact(src, "x", "x")], branch="spare"),
        _commit(client, src, [_fact(src, "x", "x")]),
        client.post(f"/v1/brains/{REF}/branches", json={"name": "new"}, headers=AUTH),
        client.delete(f"/v1/brains/{REF}/branches/spare", headers=AUTH),
        client.post(
            f"/v1/brains/{REF}/evidence",
            files={"file": ("a.txt", b"more", "text/plain")},
            data={"branch": "spare"},
            headers=AUTH,
        ),
        client.post(f"/v1/brains/{REF}/pull", headers=AUTH),
        client.post(f"/v1/brains/{REF}/converge", json={"from": "spare"}, headers=AUTH),
    ):
        assert response.status_code == 409, response.text
        body = response.json()
        assert body["code"] == "needs_human"
        assert {k: body["detail"]["convergence"][k] for k in expected} == expected

    status = client.get(f"/v1/brains/{REF}", headers=AUTH).json()["convergence"]
    assert status["actor"] == "ana@example.com" and status["started_at"]

    # The branch the client names must be the one it is open on.
    wrong = client.post(
        f"/v1/brains/{REF}/convergence/abort", json={"branch": "spare"}, headers=AUTH
    )
    assert wrong.status_code == 400 and wrong.json()["code"] == "usage"
    assert wrong.json()["detail"]["convergence"]["branch"] == "main"
    wrong = client.post(
        f"/v1/brains/{REF}/convergence/resolutions",
        json={"block": view["unresolved"][0], "kind": "admit", "branch": "spare"},
        headers=AUTH,
    )
    assert wrong.status_code == 400 and wrong.json()["code"] == "usage"

    # A writer who did not open it may not settle it; the owner may.
    client.put(
        f"/v1/brains/{REF}/writers", json={"writers": ["bob@example.com"]}, headers=AUTH
    )
    principal["value"] = BOB
    for path, body in (
        ("resolutions", {"block": view["unresolved"][0], "kind": "admit"}),
        ("accept-removals", {}),
        ("continue", None),
        ("abort", None),
    ):
        refused = client.post(
            f"/v1/brains/{REF}/convergence/{path}", json=body, headers=AUTH
        )
        assert refused.status_code == 403, (path, refused.text)
        assert refused.json()["code"] == "refused_policy"

    principal["value"] = ANA
    answered = client.post(
        f"/v1/brains/{REF}/convergence/resolutions",
        json={"block": view["unresolved"][0], "kind": "admit", "branch": "main"},
        headers=AUTH,
    )
    assert answered.status_code == 200, answered.text
    finished = client.post(
        f"/v1/brains/{REF}/convergence/continue", json={"branch": "main"}, headers=AUTH
    )
    assert finished.status_code == 200, finished.text
    assert client.app.state.lifecycle.store.get(REF).meta.convergence is None
    assert (
        _commit(client, src, [_fact(src, "y", "y")], branch="spare").status_code == 201
    )


def test_admin_settles_and_abort_takes_no_body(env) -> None:  # noqa: F811
    client, principal, *_ = env
    src = _seeded(client)
    _open_convergence(client, src)
    principal["value"] = ADMIN
    aborted = client.post(f"/v1/brains/{REF}/convergence/abort", headers=AUTH)
    assert aborted.status_code == 200, aborted.text
    assert aborted.json()["aborted"] is True
    assert client.get(f"/v1/brains/{REF}/convergence", headers=AUTH).json() == {
        "open": False
    }


# --------------------------------------------------------------------------- #
# S6: replica role
# --------------------------------------------------------------------------- #


def test_role_header_and_reader_refusal(env, monkeypatch, tmp_path) -> None:  # noqa: F811
    client, *_ = env
    assert client.get("/healthz").headers["X-GBMS-Role"] == "writer"
    assert client.get("/v1/jobs", headers=AUTH).headers["X-GBMS-Role"] == "writer"

    _configure(monkeypatch, tmp_path / "reader", store=False)
    monkeypatch.delenv("GBMS_STORE_ROOT")
    app = create_app()
    with TestClient(app) as reader:
        app.state.token_verifier.verify = AsyncMock(return_value=ANA)
        assert reader.get("/healthz").headers["X-GBMS-Role"] == "reader"
        refused = reader.post("/v1/brains", json={"reference": REF}, headers=AUTH)
        assert refused.status_code == 403
        assert refused.headers["X-GBMS-Role"] == "reader"
        assert refused.json()["detail"] == {"replica": "reader"}
        assert reader.get("/v1/jobs", headers=AUTH).json()["detail"] == {
            "replica": "reader"
        }


# --------------------------------------------------------------------------- #
# S7: the contract
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("path", "method", "param"),
    [
        ("/v1/brains/{brain_ref}/branches/{name}", "delete", "force"),
        ("/v1/brains", "get", "held"),
        ("/v1/brains", "get", "cursor"),
    ],
)
def test_openapi_documents_parameters(path: str, method: str, param: str) -> None:
    spec = json.loads(
        (Path(__file__).resolve().parent.parent / "docs" / "openapi.json").read_text()
    )
    names = [p["name"] for p in spec["paths"][path][method].get("parameters", [])]
    assert param in names


def test_openapi_documents_writers_and_inline_evidence() -> None:
    spec = json.loads(
        (Path(__file__).resolve().parent.parent / "docs" / "openapi.json").read_text()
    )
    writers = spec["paths"]["/v1/brains/{brain_ref}/writers"]
    assert {"get", "put"} <= set(writers)
    commit = spec["components"]["schemas"]["CommitRequest"]["properties"]
    assert "evidence_inline" in commit and "source" in commit
