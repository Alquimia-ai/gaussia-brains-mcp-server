from __future__ import annotations

import hashlib
import json

import pytest

from gaussia_brains_mcp_server.client import (
    BrainManifestRejected,
    GaussiaRegistryClient,
    Oras,
    manifest_digest,
    reference_target,
    validate_manifest_document,
)
from gaussia_brains_mcp_server.config import Settings

DIGEST = "sha256:" + "a" * 64


def test_oras_transport_flags_follow_the_subcommand() -> None:
    argv = Oras(Settings(oras_insecure=True, oras_plain_http=True)).argv(
        "manifest", "fetch", "r/x:v1"
    )
    assert argv[:4] == ["oras", "manifest", "fetch", "r/x:v1"]
    assert argv[4:] == ["--insecure", "--plain-http"]


def test_registry_password_goes_through_stdin_not_argv() -> None:
    from pydantic import SecretStr

    oras = Oras(Settings(registry_username="bot", registry_password=SecretStr("s3cret")))
    argv = oras.argv("repo", "tags", "r/x")
    assert "s3cret" not in argv
    assert argv[-3:] == ["--username", "bot", "--password-stdin"]
    assert oras._stdin() == "s3cret"


def test_registry_config_is_forwarded() -> None:
    argv = Oras(Settings(registry_config="/run/secrets/docker.json")).argv(
        "repo", "tags", "r/x"
    )
    assert argv[-2:] == ["--registry-config", "/run/secrets/docker.json"]


def test_reference_target_handles_tags_and_digests() -> None:
    assert reference_target("r/x", "v1") == "r/x:v1"
    assert reference_target("r/x", DIGEST) == f"r/x@{DIGEST}"


def test_manifest_digest_hashes_bytes_as_served() -> None:
    raw = b'{"a": 1}\r\n'
    assert manifest_digest(raw) == "sha256:" + hashlib.sha256(raw).hexdigest()


def test_non_brain_manifests_are_rejected() -> None:
    with pytest.raises(BrainManifestRejected, match="not a Boltzmann brain"):
        validate_manifest_document(
            {"artifactType": "application/vnd.oci.image"}, "r/x:v1"
        )


async def test_pinned_digest_must_match_served_bytes(monkeypatch) -> None:
    """A registry serving other bytes for a pinned digest is refused."""
    client = GaussiaRegistryClient(settings=Settings())

    async def fake_fetch(*args, what):
        assert args[-1] == f"r/x@{DIGEST}"
        return json.dumps({"artifactType": "x"}).encode()

    monkeypatch.setattr(client._oras, "run_bytes", fake_fetch)
    with pytest.raises(BrainManifestRejected, match="hashing to"):
        await client.resolve("r/x", DIGEST)
