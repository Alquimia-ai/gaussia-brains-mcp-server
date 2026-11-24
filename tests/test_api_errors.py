"""The REST error contract at its edges: parsing, auth, uploads, names, models."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pydantic
import pytest
from boltzmann.exceptions import BranchNotFoundError
from boltzmann.exceptions import ValidationError as BoltzmannValidationError
from fastapi.testclient import TestClient

from conftest import ISSUER, RESOURCE
from gaussia_brains_mcp_server.api import lifecycle as api
from gaussia_brains_mcp_server.auth import scopes
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.config import get_settings
from gaussia_brains_mcp_server.lifecycle import errors
from gaussia_brains_mcp_server.main import create_app

REF = "registry.example.com/acme/handbook"
AUTH = {"Authorization": "Bearer token"}
WRITER = Principal(
    subject="0b9e-ana",
    issuer=ISSUER,
    scopes=frozenset({scopes.WRITE, scopes.CATALOG, scopes.READ}),
    claims={"email": "ana@example.com", "name": "Ana"},
)
READER = Principal(subject="r", issuer=ISSUER, scopes=frozenset({scopes.CATALOG}))
EVIDENCE_LIMIT = 1000
IMPORT_LIMIT = 2000


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    get_settings.cache_clear()
    monkeypatch.setenv("GBMS_AUTH_RESOURCE", RESOURCE)
    monkeypatch.setenv("GBMS_AUTH_ISSUERS", json.dumps([{"issuer": ISSUER}]))
    monkeypatch.setenv(
        "GBMS_ALLOWED_SOURCES", json.dumps(["registry.example.com/acme"])
    )
    monkeypatch.setenv("GBMS_STORE_ROOT", str(tmp_path / "store"))
    monkeypatch.setenv("GBMS_STORE_MAX_EVIDENCE_BYTES", str(EVIDENCE_LIMIT))
    monkeypatch.setenv("GBMS_STORE_MAX_IMPORT_BYTES", str(IMPORT_LIMIT))
    app = create_app()
    with TestClient(app) as client:
        principal = {"value": WRITER}
        app.state.token_verifier.verify = AsyncMock(
            side_effect=lambda _t: principal["value"]
        )
        yield client, principal, app


def _assert_error(response, status: int, code: str) -> dict[str, Any]:
    assert response.status_code == status, response.text
    body = response.json()
    assert body["code"] == code, body
    assert set(body) == {"code", "message", "detail"}
    return body


def _create(client: TestClient, ref: str = REF) -> None:
    response = client.post("/v1/brains", json={"reference": ref}, headers=AUTH)
    assert response.status_code == 201, response.text


def _evidence(client: TestClient, text: str = "a source") -> dict[str, Any]:
    response = client.post(
        f"/v1/brains/{REF}/evidence",
        files={"file": ("s.txt", text.encode(), "text/plain")},
        headers=AUTH,
    )
    assert response.status_code == 201, response.text
    return response.json()


# -- 1. request validation is usage (400), not internal ----------------------- #


def test_request_validation_is_usage(env) -> None:
    client, _, _ = env
    body = _assert_error(client.post("/v1/brains", json={}, headers=AUTH), 400, "usage")
    assert body["detail"]["errors"][0]["loc"] == ["body", "reference"]
    _create(client)
    _assert_error(
        client.get(f"/v1/brains/{REF}/history?limit=0", headers=AUTH), 400, "usage"
    )


# -- 2. mapping by class, not by name ------------------------------------------ #


def test_import_body_of_the_wrong_type_is_usage(env) -> None:
    client, _, _ = env
    response = client.post(f"/v1/brains/{REF}/import", json={"tag": 5}, headers=AUTH)
    body = _assert_error(response, 400, "usage")
    assert body["detail"]["errors"][0]["loc"] == ["tag"]


def test_describe_maps_by_class() -> None:
    class Model(pydantic.BaseModel):
        x: int

    with pytest.raises(pydantic.ValidationError) as caught:
        Model.model_validate({"x": "no"})
    assert errors.describe(caught.value).code == "internal"
    assert errors.describe(BoltzmannValidationError("x")).code == "refused_protocol"
    assert errors.describe(ValueError("bug")).code == "internal"
    assert errors.describe(KeyError("bug")).code == "internal"


# -- 3. names that would be shadowed by routes --------------------------------- #


@pytest.mark.parametrize(
    "ref",
    [
        "registry.example.com/acme/blocks",
        "registry.example.com/history/x",
        "registry.example.com/acme/branches:latest",
    ],
)
def test_reserved_brain_names_are_refused(env, ref: str) -> None:
    client, _, _ = env
    response = client.post("/v1/brains", json={"reference": ref}, headers=AUTH)
    _assert_error(response, 400, "usage")
    response = client.post(f"/v1/brains/{ref}/import", json={}, headers=AUTH)
    _assert_error(response, 400, "usage")


@pytest.mark.parametrize("name", ["a/branches/b", "branches", "x/push", "push"])
def test_unaddressable_branch_names_are_refused(env, name: str) -> None:
    client, _, _ = env
    _create(client)
    response = client.post(
        f"/v1/brains/{REF}/branches", json={"name": name}, headers=AUTH
    )
    _assert_error(response, 400, "usage")


# -- 4. uploads: authenticated first, bounded always ----------------------------- #


def test_evidence_form_is_not_read_before_auth(env, monkeypatch) -> None:
    client, _, _ = env
    _create(client)
    calls: list[int] = []
    real = api._read_form

    async def spy(request, limit):
        calls.append(limit)
        return await real(request, limit)

    monkeypatch.setattr(api, "_read_form", spy)
    # A malformed multipart body: parsed first, it would be a 400.
    response = client.post(
        f"/v1/brains/{REF}/evidence",
        content=b"not multipart at all",
        headers={"Content-Type": "multipart/form-data; boundary=zzz"},
    )
    body = _assert_error(response, 401, "unauthenticated")
    assert "www-authenticate" in response.headers
    assert body["detail"] == {}
    assert calls == []


def test_evidence_past_the_limit_is_413(env) -> None:
    client, _, _ = env
    _create(client)
    big = b"x" * (EVIDENCE_LIMIT + api.FORM_OVERHEAD_BYTES + 1)
    # Declared length over the limit: refused before reading.
    response = client.post(
        f"/v1/brains/{REF}/evidence",
        content=big,
        headers={**AUTH, "Content-Type": "multipart/form-data; boundary=zzz"},
    )
    _assert_error(response, 413, "usage")
    # Streamed (no Content-Length): refused once the limit is passed.
    response = client.post(
        f"/v1/brains/{REF}/evidence",
        content=iter([big[i : i + 65536] for i in range(0, len(big), 65536)]),
        headers={**AUTH, "Content-Type": "multipart/form-data; boundary=zzz"},
    )
    _assert_error(response, 413, "usage")
    # A well-formed form whose file alone is over the limit.
    response = client.post(
        f"/v1/brains/{REF}/evidence",
        files={"file": ("s.txt", b"y" * (EVIDENCE_LIMIT + 1), "text/plain")},
        headers=AUTH,
    )
    _assert_error(response, 413, "usage")
    # Under the limit, the form fields still arrive.
    response = client.post(
        f"/v1/brains/{REF}/evidence",
        files={"file": ("s.txt", b"small", "text/plain")},
        data={"media_type": "text/markdown", "origin": "https://example.com/s"},
        headers=AUTH,
    )
    assert response.status_code == 201, response.text
    assert response.json()["media_type"] == "text/markdown"


def test_import_upload_past_the_limit_is_413(env) -> None:
    client, _, _ = env
    response = client.post(
        f"/v1/brains/{REF}/import",
        files={"layout": ("l.tar", b"z" * (IMPORT_LIMIT + 1), "application/x-tar")},
        headers=AUTH,
    )
    _assert_error(response, 413, "usage")
    big = b"z" * (IMPORT_LIMIT + api.FORM_OVERHEAD_BYTES + 1)
    response = client.post(
        f"/v1/brains/{REF}/import",
        content=iter([big[i : i + 65536] for i in range(0, len(big), 65536)]),
        headers={**AUTH, "Content-Type": "multipart/form-data; boundary=zzz"},
    )
    _assert_error(response, 413, "usage")


# -- 5. GET /{ref} speaks the contract ----------------------------------------- #


def test_brain_status_errors_carry_codes(env, monkeypatch) -> None:
    client, _, app = env
    _create(client)
    service = app.state.lifecycle

    def missing(*_a, **_k):
        raise BranchNotFoundError("no branch named 'x'")

    monkeypatch.setattr(service, "status", missing)
    _assert_error(client.get(f"/v1/brains/{REF}", headers=AUTH), 404, "not_found")

    def broken(*_a, **_k):
        raise RuntimeError("bug")

    monkeypatch.setattr(service, "status", broken)
    _assert_error(client.get(f"/v1/brains/{REF}", headers=AUTH), 500, "internal")


# -- 6. auth failures speak the contract --------------------------------------- #


def test_auth_failures_carry_codes(env) -> None:
    client, principal, _ = env
    response = client.post("/v1/brains", json={"reference": REF})
    _assert_error(response, 401, "unauthenticated")
    assert response.headers["www-authenticate"].startswith("Bearer ")
    principal["value"] = READER
    response = client.post("/v1/brains", json={"reference": REF}, headers=AUTH)
    _assert_error(response, 403, "refused_policy")
    assert 'error="insufficient_scope"' in response.headers["www-authenticate"]


# -- 7. If-Match on branch writes ------------------------------------------------ #


def test_if_match_guards_branch_creation(env) -> None:
    client, _, _ = env
    _create(client)
    head = _evidence(client)["head"]
    stale = "sha256:" + "0" * 64
    response = client.post(
        f"/v1/brains/{REF}/branches",
        json={"name": "draft"},
        headers={**AUTH, "If-Match": f'"{stale}"'},
    )
    _assert_error(response, 409, "not_fast_forward")
    response = client.post(
        f"/v1/brains/{REF}/branches",
        json={"name": "draft"},
        headers={**AUTH, "If-Match": f'"{head}"'},
    )
    assert response.status_code == 201, response.text


# -- 8. typed responses ------------------------------------------------------------ #


def test_read_responses_are_described() -> None:
    spec = create_app().openapi()
    paths = spec["paths"]

    def schema(path: str, method: str) -> dict[str, Any]:
        ok = paths[path][method]["responses"]
        return ok["200"]["content"]["application/json"]["schema"]

    assert schema("/v1/brains/{brain_ref}/search", "post")["$ref"].endswith(
        "/SearchResponse"
    )
    assert schema("/v1/brains/{brain_ref}/history", "get")["$ref"].endswith(
        "/HistoryPage"
    )
    assert schema("/v1/brains/{brain_ref}/commits", "post")["$ref"].endswith(
        "/CommitResponse"
    )
    search = spec["components"]["schemas"]["SearchResponse"]
    assert {"truncated", "narrowing"} <= set(search["required"])
    for status in ("401", "413", "500"):
        assert status in paths["/v1/brains/{brain_ref}/evidence"]["post"]["responses"]


def test_value_error_from_pyboltzmann_is_usage() -> None:
    """pyboltzmann rejects bad client records with ValueError: that is ``usage``."""
    from boltzmann.identity.time import parse_timestamp  # noqa: PLC0415

    try:
        parse_timestamp("not a timestamp")
    except ValueError as exc:
        assert errors.describe(exc).code == "usage"
    else:  # pragma: no cover - the SDK accepted garbage
        raise AssertionError("expected ValueError")
