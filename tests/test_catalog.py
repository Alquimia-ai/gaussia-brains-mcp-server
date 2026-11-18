from __future__ import annotations

import asyncio
from typing import Any

import pytest
from boltzmann.distribution.manifest import Descriptor

from gaussia_brains_mcp_server import catalog as catalog_module
from gaussia_brains_mcp_server.catalog import Catalog, _annotation
from gaussia_brains_mcp_server.client import ENTRY_ARTIFACT_TYPE, repo_path
from gaussia_brains_mcp_server.config import Settings

NS = "registry.example.com/brains"


def _digest(ch: str) -> str:
    return "sha256:" + ch * 64


def _entry(ch: str, repo: str, *, modules: str | None = "semantic") -> Descriptor:
    annotations = {
        _annotation("kind"): "brain",
        _annotation("repo"): repo,
        _annotation("tag"): "latest",
        _annotation("digest"): _digest(ch),
    }
    if modules is not None:
        annotations[_annotation("modules")] = modules
    return Descriptor.model_validate(
        {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            # The entry artifact's own digest, distinct from the brain it names.
            "digest": "sha256:" + "0" * 63 + ch,
            "size": 1,
            "artifactType": ENTRY_ARTIFACT_TYPE,
            "annotations": annotations,
        }
    )


class FakeClient:
    def __init__(self, entries: list[Descriptor], *, delay: float = 0.0) -> None:
        self.entries = entries
        self.delay = delay
        self.index_calls = 0
        self.manifest_calls: list[str] = []
        self.in_flight = 0
        self.max_in_flight = 0

    async def index_entries(self, namespace: str) -> list[Descriptor]:
        self.index_calls += 1
        return self.entries

    async def manifest_modules(self, reference: str, digest: str) -> list[str]:
        self.manifest_calls.append(digest)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        await asyncio.sleep(self.delay)
        self.in_flight -= 1
        if reference.endswith("broken"):
            raise RuntimeError("gone")
        return ["procedural"]


@pytest.fixture(autouse=True)
def _fresh_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(catalog_module, "_index_cache", catalog_module._IndexCache())


def _catalog(client: FakeClient, **settings: Any) -> Catalog:
    cat = Catalog(Settings(**settings), client_factory=lambda pinned: client)
    return cat


async def test_listing_reads_only_the_index_when_entries_carry_modules() -> None:
    client = FakeClient([_entry("a", "brains/one"), _entry("b", "brains/two")])
    entries, cursor = await _catalog(client).list_entries(NS, base_url="http://h/mcp")
    assert [e.reference for e in entries] == [f"{NS}/one", f"{NS}/two"]
    assert entries[0].modules == ["semantic"]
    assert entries[0].mcp_pinned == f"http://h/mcp/{NS}/one@{_digest('a')}"
    assert client.index_calls == 1
    assert client.manifest_calls == []
    assert cursor is None


async def test_legacy_entries_resolve_concurrently_and_cache_by_digest() -> None:
    legacy = [_entry(c, f"brains/b{c}", modules=None) for c in "abcd"]
    client = FakeClient(legacy, delay=0.05)
    cat = _catalog(client, catalog_index_ttl_seconds=0)

    entries, _ = await cat.list_entries(NS, base_url="http://h/mcp")
    assert [e.modules for e in entries] == [["procedural"]] * 4
    assert client.max_in_flight == 4

    await cat.list_entries(NS, base_url="http://h/mcp")
    assert len(client.manifest_calls) == 4  # second listing hit the digest cache
    assert client.index_calls == 2  # ttl=0 reads the anchor every time


async def test_index_listing_is_reused_within_ttl() -> None:
    client = FakeClient([_entry("a", "brains/one")])
    cat = _catalog(client, catalog_index_ttl_seconds=60)
    await cat.list_entries(NS, base_url="http://h/mcp")
    await cat.list_entries(NS, base_url="http://h/mcp")
    assert client.index_calls == 1


async def test_pagination_skips_unresolvable_entries_and_keeps_order() -> None:
    client = FakeClient(
        [
            _entry("a", "brains/one"),
            _entry("b", "brains/broken", modules=None),
            _entry("c", "brains/three"),
            _entry("d", "brains/four"),
        ]
    )
    cat = _catalog(client)
    page, cursor = await cat.list_entries(NS, base_url="http://h/mcp", limit=2)
    assert [e.reference for e in page] == [f"{NS}/one", f"{NS}/three"]
    assert cursor is not None

    rest, cursor = await cat.list_entries(
        NS, base_url="http://h/mcp", limit=2, cursor=cursor
    )
    assert [e.reference for e in rest] == [f"{NS}/four"]
    assert cursor is None


async def test_fully_qualified_repo_annotations_are_listed() -> None:
    """Entries written with the registry host in ``repo`` still belong to the namespace."""
    client = FakeClient([_entry("a", f"{NS}/one"), _entry("b", "other/two")])
    entries, _ = await _catalog(client).list_entries(NS, base_url="http://h/mcp")
    assert [e.reference for e in entries] == [f"{NS}/one"]


def test_repo_path_strips_only_the_namespace_host() -> None:
    assert repo_path(NS, f"{NS}/one") == "brains/one"
    assert repo_path(NS, "brains/one") == "brains/one"
    assert (
        repo_path(NS, "other.example.com/brains/one") == "other.example.com/brains/one"
    )
