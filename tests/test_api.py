from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from conftest import ISSUER, RESOURCE
from gaussia_brains_mcp_server.auth import scopes
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.config import get_settings
from gaussia_brains_mcp_server.main import create_app


def _configure_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    get_settings.cache_clear()
    monkeypatch.setenv("GBMS_AUTH_RESOURCE", RESOURCE)
    monkeypatch.setenv(
        "GBMS_AUTH_ISSUERS",
        json.dumps(
            [
                {
                    "issuer": ISSUER,
                    "claims": {"roles": ["realm_access.roles"]},
                    "role_scopes": {"brain-reader": ["brains:read", "brains:catalog"]},
                }
            ]
        ),
    )


def test_healthz(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_auth(monkeypatch)
    with TestClient(create_app()) as client:
        response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_brain_tags_requires_catalog_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_auth(monkeypatch)
    app = create_app()
    with TestClient(app) as client:
        app.state.token_verifier.verify = AsyncMock(
            return_value=Principal(subject="u", issuer=ISSUER, scopes={scopes.READ})
        )
        response = client.get(
            "/v1/brains/registry.example.com/acme/handbook:latest/tags",
            headers={"Authorization": "Bearer token"},
        )
    assert response.status_code == 403


def test_brain_tags_returns_tag_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_auth(monkeypatch)
    app = create_app()
    with TestClient(app) as client:
        app.state.token_verifier.verify = AsyncMock(
            return_value=Principal(subject="u", issuer=ISSUER, scopes={scopes.CATALOG})
        )
        with patch(
            "gaussia_brains_mcp_server.catalog.Catalog.list_tags",
            return_value=["latest", "v1.0.0"],
        ):
            response = client.get(
                "/v1/brains/registry.example.com/acme/handbook:latest/tags",
                headers={"Authorization": "Bearer token"},
            )
    assert response.status_code == 200
    body = response.json()
    assert body["tags"] == ["latest", "v1.0.0"]


def test_refuses_to_start_with_auth_on_and_no_issuers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GBMS_AUTH_ENABLED", "true")
    monkeypatch.setenv("GBMS_AUTH_ISSUERS", "[]")
    monkeypatch.delenv("GBMS_ALLOW_INSECURE_DEV", raising=False)
    get_settings.cache_clear()
    with pytest.raises(ValueError, match="GBMS_AUTH_ISSUERS"):
        with TestClient(create_app()):
            pass
    get_settings.cache_clear()
