from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from gaussia_brains_mcp_server.client import (
    ENTRY_ARTIFACT_TYPE,
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

    oras = Oras(
        Settings(registry_username="bot", registry_password=SecretStr("s3cret"))
    )
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


async def test_attach_index_entry_runs_in_temp_dir(monkeypatch) -> None:
    """The entry payload is written to a temp dir, so oras must run there.

    Regression: a relative ``entry.json`` was resolved against the process
    working directory (``/app`` in the container), and an absolute path is
    rejected by ``oras attach`` for security. We keep the relative file name
    and set the subprocess cwd to the temp directory.
    """
    client = GaussiaRegistryClient(settings=Settings())
    captured: dict[str, Any] = {}

    async def fake_ensure_index(namespace: str) -> str:
        return "registry.example/brains/index:v1"

    async def fake_detach_index_entry(
        namespace: str, repo: str, tag: str, *, quiet: bool = False
    ) -> int:
        return 0

    async def fake_run(*args: str, what: str, cwd: str | None = None) -> str:
        captured["args"] = args
        captured["what"] = what
        captured["cwd"] = cwd
        return f"Attached digest: sha256:{'b' * 64}"

    monkeypatch.setattr(client, "ensure_index", fake_ensure_index)
    monkeypatch.setattr(client, "detach_index_entry", fake_detach_index_entry)
    monkeypatch.setattr(client._oras, "run", fake_run)

    result = await client.attach_index_entry(
        namespace="registry.example/brains",
        repo="registry.example/brains/foo",
        tag="latest",
        digest=DIGEST,
    )

    file_args = [
        a for a in captured["args"] if a.endswith(f":{ENTRY_ARTIFACT_TYPE}+json")
    ]
    assert len(file_args) == 1
    assert file_args[0] == f"entry.json:{ENTRY_ARTIFACT_TYPE}+json"
    assert captured["cwd"] is not None
    assert (
        "entry.json" in Path(captured["cwd"]).name.lower() or "tmp" in captured["cwd"]
    )
    assert result == f"sha256:{'b' * 64}"
