"""Brain cache behaviour, with the registry and pyboltzmann pulls stubbed out."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from boltzmann.authenticity import AuthorshipState, UnsignedPolicy, VerificationPolicy
from boltzmann.exceptions import AuthenticityError, DistributionError

from gaussia_brains_mcp_server.cache import (
    BrainCache,
    BrainUnavailableError,
    SourceNotAllowedError,
    split_reference,
)
from gaussia_brains_mcp_server.client import BrainManifestRejected
from gaussia_brains_mcp_server.config import Settings


class FakeBrain:
    def __init__(
        self, name: str, state: AuthorshipState = AuthorshipState.AUTHORIZED
    ) -> None:
        self.name = name
        self.state = state
        self.trust_root = None

    def authenticate(self, *_: Any, **__: Any) -> SimpleNamespace:
        return SimpleNamespace(state=self.state)


class FakeRegistry:
    """Plays the remote: what `resolve` answers, and whether it is reachable."""

    def __init__(self) -> None:
        self.digest = "sha256:" + "a" * 64
        self.down = False
        self.rejected = False
        self.resolves = 0

    def factory(self, modules=None, *, pinned: bool = False) -> FakeRegistry:
        return self

    async def resolve(self, reference: str, tag: str) -> SimpleNamespace:
        self.resolves += 1
        if self.down:
            raise DistributionError("registry unreachable")
        if self.rejected:
            raise BrainManifestRejected("not a brain")
        return SimpleNamespace(
            config=SimpleNamespace(digest=self.digest), digest=self.digest
        )

    async def referrers(self, *_: Any) -> list:
        return []


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class StubCache(BrainCache):
    """`_open` replaced: counts opens, follows the fake registry, can be made to fail."""

    def __init__(self, registry: FakeRegistry, clock: Clock, **settings: Any) -> None:
        base = {
            "cache_ttl_seconds": 60,
            "cache_max_brains": 4,
            "cache_failure_backoff_seconds": 30,
            "allowed_sources": ["registry.example.com/brains", "r"],
        }
        base.update(settings)
        super().__init__(Settings(**base), client_factory=registry.factory, clock=clock)  # type: ignore[arg-type]
        self.registry = registry
        self.opens: list[tuple[str, str, Any]] = []
        self.fail_with: Exception | None = None
        self.next_state = AuthorshipState.AUTHORIZED
        self.delay = 0.0

    async def _open(self, reference, tag, modules, verification):
        self.opens.append((reference, tag, modules))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail_with is not None:
            raise self.fail_with
        if self.registry.down:
            raise DistributionError("registry unreachable")
        return FakeBrain(
            f"{reference}:{tag}#{len(self.opens)}", self.next_state
        ), self.registry.digest


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def cache(clock: Clock) -> StubCache:
    return StubCache(FakeRegistry(), clock)


REF = "registry.example.com/brains/acme:v1"


def test_split_reference_handles_ports_and_defaults() -> None:
    assert split_reference("localhost:5000/brains/x") == (
        "localhost:5000/brains/x",
        "latest",
    )
    assert split_reference("localhost:5000/brains/x:v2") == (
        "localhost:5000/brains/x",
        "v2",
    )
    assert split_reference("ghcr.io/a/b:main") == ("ghcr.io/a/b", "main")
    digest = "sha256:" + "c" * 64
    assert split_reference(f"localhost:5000/b/x@{digest}") == (
        "localhost:5000/b/x",
        digest,
    )
    assert split_reference(f"ghcr.io/a/b:main@{digest}") == ("ghcr.io/a/b", digest)
    with pytest.raises(ValueError):
        split_reference("ghcr.io/a/b@md5:abc")


async def test_hit_within_ttl_does_not_touch_registry(
    cache: StubCache, clock: Clock
) -> None:
    first = await cache.get_or_open(REF)
    clock.now += 30
    assert await cache.get_or_open(REF) is first
    assert len(cache.opens) == 1
    assert cache.registry.resolves == 0


async def test_concurrent_first_opens_are_single_flight(cache: StubCache) -> None:
    cache.delay = 0.01
    brains = await asyncio.gather(*(cache.get_or_open(REF) for _ in range(10)))
    assert len(cache.opens) == 1
    assert all(b is brains[0] for b in brains)


async def test_new_entry_is_never_its_own_eviction_victim(clock: Clock) -> None:
    """Regression: the placeholder (checked_at=0) used to be evicted first."""
    cache = StubCache(FakeRegistry(), clock, cache_max_brains=1)
    cache.delay = 0.01
    await cache.get_or_open("r/a:v1")
    clock.now += 1
    results = await asyncio.gather(*(cache.get_or_open("r/b:v1") for _ in range(5)))
    assert [o[0] for o in cache.opens] == ["r/a", "r/b"]  # one open for b, not five
    assert all(r is results[0] for r in results)
    assert [e["reference"] for e in cache.stats()["entries"]] == ["r/b"]


async def test_one_entry_per_reference_whatever_the_caller() -> None:
    """Headless: nothing a caller passes narrows what is pulled or keyed."""
    cache = StubCache(FakeRegistry(), Clock())
    a = await cache.get_or_open(REF)
    b = await cache.get_or_open("registry.example.com/brains/acme", tag="v1")
    assert a is b and len(cache.opens) == 1
    assert cache.opens[0][2] is None  # every module


PINNED = "registry.example.com/brains/acme@sha256:" + "f" * 64


async def test_pinned_digest_is_never_revalidated(
    cache: StubCache, clock: Clock
) -> None:
    first = await cache.get_or_open(PINNED)
    cache.registry.digest = "sha256:" + "b" * 64
    clock.now += 10_000
    assert await cache.get_or_open(PINNED) is first
    assert cache.registry.resolves == 0
    assert cache.opens[0][1] == "sha256:" + "f" * 64


async def test_tag_and_digest_connections_are_separate_entries(
    cache: StubCache,
) -> None:
    await cache.get_or_open(REF)
    await cache.get_or_open(PINNED)
    assert {e["tag"] for e in cache.stats()["entries"]} == {"v1", "sha256:" + "f" * 64}


async def test_ttl_revalidates_without_repull_when_digest_unchanged(
    cache: StubCache, clock: Clock
) -> None:
    first = await cache.get_or_open(REF)
    clock.now += 61
    assert await cache.get_or_open(REF) is first
    assert cache.registry.resolves == 1
    assert len(cache.opens) == 1


async def test_digest_move_repulls_and_swaps(cache: StubCache, clock: Clock) -> None:
    first = await cache.get_or_open(REF)
    cache.registry.digest = "sha256:" + "b" * 64
    clock.now += 61
    second = await cache.get_or_open(REF)
    assert second is not first
    assert len(cache.opens) == 2


async def test_registry_outage_after_ttl_serves_held_brain(
    cache: StubCache, clock: Clock
) -> None:
    """Regression: the port re-opened from scratch and failed during outages."""
    first = await cache.get_or_open(REF)
    cache.registry.down = True
    clock.now += 61
    assert await cache.get_or_open(REF) is first
    # And it does not hammer the registry on every request afterwards.
    clock.now += 1
    assert await cache.get_or_open(REF) is first
    assert cache.registry.resolves == 1


async def test_rejected_manifest_keeps_held_brain(
    cache: StubCache, clock: Clock
) -> None:
    first = await cache.get_or_open(REF)
    cache.registry.rejected = True
    clock.now += 61
    assert await cache.get_or_open(REF) is first


async def test_failed_repull_keeps_held_brain(cache: StubCache, clock: Clock) -> None:
    first = await cache.get_or_open(REF)
    cache.registry.digest = "sha256:" + "c" * 64
    cache.fail_with = AuthenticityError("new head is not authored")
    clock.now += 61
    assert await cache.get_or_open(REF) is first


async def test_failed_repull_does_not_serve_a_head_the_policy_refuses(
    cache: StubCache, clock: Clock
) -> None:
    cache.next_state = AuthorshipState.UNAUTHORIZED
    await cache.get_or_open(REF)
    cache.registry.digest = "sha256:" + "d" * 64
    cache.fail_with = DistributionError("blob fetch failed")
    clock.now += 61
    with pytest.raises(DistributionError):
        await cache.get_or_open(REF)


async def test_first_open_failure_backs_off(cache: StubCache, clock: Clock) -> None:
    cache.fail_with = DistributionError("registry unreachable")
    with pytest.raises(DistributionError):
        await cache.get_or_open(REF)
    cache.fail_with = None
    with pytest.raises(BrainUnavailableError):
        await cache.get_or_open(REF)  # inside the 30s window: no attempt
    assert len(cache.opens) == 1
    clock.now += 31
    await cache.get_or_open(REF)
    assert len(cache.opens) == 2


async def test_backoff_grows_exponentially_and_is_capped(clock: Clock) -> None:
    cache = StubCache(FakeRegistry(), clock, cache_failure_backoff_max_seconds=100)
    cache.fail_with = DistributionError("down")
    windows = []
    for _ in range(4):
        with pytest.raises(DistributionError):
            await cache.get_or_open(REF)
        entry = next(iter(cache._entries.values()))
        windows.append(entry.retry_after - clock.now)
        clock.now = entry.retry_after
    assert windows == [30, 60, 100, 100]


async def test_different_verification_policy_rejudges_held_brain(
    cache: StubCache,
) -> None:
    await cache.get_or_open(REF)
    stricter = VerificationPolicy(unsigned=UnsignedPolicy.REFUSE, required_signatures=2)
    await cache.get_or_open(REF, verification=stricter)
    assert len(cache.opens) == 2
    await cache.get_or_open(REF, verification=stricter)
    assert len(cache.opens) == 2  # judged once under the new policy


async def test_invalidate_drops_all_tags_of_a_reference(cache: StubCache) -> None:
    await cache.get_or_open("r/a:v1")
    await cache.get_or_open("r/a:v2")
    await cache.get_or_open("r/b:v1")
    assert cache.invalidate("r/a") == 2
    assert [e["reference"] for e in cache.stats()["entries"]] == ["r/b"]


async def test_references_outside_allowed_sources_are_refused(cache: StubCache) -> None:
    for ref in ("evil.example.com/brains/x:v1", "registry.example.com/brainsx/y:v1"):
        with pytest.raises(SourceNotAllowedError):
            await cache.get_or_open(ref)
    assert cache.opens == []


async def test_no_sources_configured_refuses_everything(clock: Clock) -> None:
    cache = StubCache(FakeRegistry(), clock, allowed_sources=[])
    with pytest.raises(SourceNotAllowedError, match="none configured"):
        await cache.get_or_open(REF)


def test_catalog_namespaces_are_the_default_sources() -> None:
    s = Settings(catalog_namespaces=["registry.example.com/brains/"])
    assert s.source_allowed("registry.example.com/brains/acme")
    assert not s.source_allowed("registry.example.com/other/acme")
