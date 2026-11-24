"""Concurrency of the lifecycle layer: locks, cancels, cleanup and If-Match.

These run the service directly over a real working store; the registry is
pyboltzmann's ``LocalLayoutRegistry`` where one is needed.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any

import pytest
from boltzmann.distribution.local import LocalLayoutRegistry

from conftest import ISSUER
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.config import Settings
from gaussia_brains_mcp_server.lifecycle import errors
from gaussia_brains_mcp_server.lifecycle import service as service_module
from gaussia_brains_mcp_server.lifecycle.identity import actor_for
from gaussia_brains_mcp_server.lifecycle.jobs import JobRunner, JobState
from gaussia_brains_mcp_server.lifecycle.service import LifecycleService
from gaussia_brains_mcp_server.lifecycle.workstore import (
    LAYOUT_DIR,
    BrainMeta,
    WorkingStore,
    run_locked_thread,
)

REF = "registry.example.com/acme/handbook"


class StubCatalog:
    async def publish(self, ref: str, **kwargs: Any) -> dict[str, Any]:
        return {"entry_digest": "sha256:" + "e" * 64}


@pytest.fixture
def service(tmp_path: Path) -> LifecycleService:
    settings = Settings(
        store_root=str(tmp_path / "store"),
        allowed_sources=["registry.example.com/acme"],
    )
    registry = LocalLayoutRegistry(tmp_path / "registry")
    return LifecycleService(
        settings,
        WorkingStore(settings),
        JobRunner(),
        client_factory=lambda *a, **k: registry,
        catalog_factory=lambda _s: StubCatalog(),
    )


@pytest.fixture
def actor():
    principal = Principal(
        subject="ana", issuer=ISSUER, claims={"email": "ana@example.com"}
    )
    return actor_for(principal, Settings())


def _fact(source: str, label: str, statement: str, subject: str | None = None):
    payload = {"kind": "fact", "label": label, "statement": statement}
    if subject:
        payload["subject"] = subject
    return {"memory_type": "semantic", "payload": payload, "evidence": [source]}


async def _seed(service: LifecycleService, actor) -> tuple[Any, str, str]:
    """A brain with one commit on main: ``(handle, source block, head)``."""
    service.create(REF, actor=actor)
    handle = service.handle(REF)
    source = (
        await service.register_evidence(
            handle, b"Capitals.", actor=actor, media_type="text/plain"
        )
    )["block_id"]
    done = await service.commit(
        handle,
        actor=actor,
        source=source,
        candidates=[_fact(source, "intro", "Intro.")],
        producer={"kind": "model", "id": "openai/gpt-4.1"},
    )
    return handle, source, done["head"]


# --------------------------------------------------------------------------- #
# Worker threads under the lock
# --------------------------------------------------------------------------- #


async def test_cancel_waits_for_the_worker_thread_before_releasing_the_lock() -> None:
    lock = asyncio.Lock()
    started = threading.Event()
    release = threading.Event()
    finished: list[bool] = []

    def work() -> None:
        started.set()
        release.wait(5)
        finished.append(True)

    async def writer() -> None:
        async with lock:
            await run_locked_thread(work)

    task = asyncio.create_task(writer())
    await asyncio.to_thread(started.wait, 5)
    task.cancel()
    await asyncio.sleep(0.05)
    # Cancelled, but the thread is still running: the lock must still be held.
    assert lock.locked() and not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished == [True]
    assert not lock.locked()


async def test_commit_point_from_a_worker_thread_races_cancel_safely() -> None:
    runner = JobRunner()
    past = threading.Event()
    go_on = threading.Event()

    def work(ctx) -> dict[str, Any]:
        ctx.commit_point("writing")
        past.set()
        go_on.wait(5)
        return {"ok": True}

    async def body(ctx):
        return await run_locked_thread(work, ctx)

    job = runner.submit("push", brain="b", actor="a", body=body)
    await asyncio.to_thread(past.wait, 5)
    with pytest.raises(errors.LifecycleError) as caught:
        runner.cancel(job.id)
    assert caught.value.code == "usage"
    go_on.set()
    await job.task
    assert job.state is JobState.SUCCEEDED


async def test_async_cleanup_is_awaited() -> None:
    runner = JobRunner()
    cleaned: list[bool] = []

    async def body(_ctx):
        raise RuntimeError("boom")

    async def cleanup() -> None:
        await asyncio.sleep(0)
        cleaned.append(True)

    job = runner.submit("import", brain="b", actor="a", body=body, cleanup=cleanup)
    await job.task
    assert job.state is JobState.FAILED and cleaned == [True]


# --------------------------------------------------------------------------- #
# Import cleanup
# --------------------------------------------------------------------------- #


async def test_discard_waits_for_the_write_lock(
    service: LifecycleService, actor
) -> None:
    store = service.store
    handle = store.create(BrainMeta(reference=REF))
    written = asyncio.Event()
    release = asyncio.Event()

    async def other_client() -> None:
        async with handle.writing("registering evidence"):
            brain = handle.writer()
            await asyncio.to_thread(
                service_module.LifecycleService._require_writable, brain
            )
            written.set()
            await release.wait()

    writer = asyncio.create_task(other_client())
    await written.wait()
    discarding = asyncio.create_task(store.discard_if_empty(handle))
    await asyncio.sleep(0.05)
    assert not discarding.done()  # it waits for the write lock
    release.set()
    await writer
    # Nothing was committed by the stub writer: the brain is still empty.
    assert await discarding is True
    assert not store.exists(REF)


async def test_discard_rechecks_under_the_lock(
    service: LifecycleService, actor
) -> None:
    service.create(REF, actor=actor)
    handle = service.handle(REF)
    gate = asyncio.Event()

    async def slow_write() -> None:
        async with handle.writing("committing"):
            await gate.wait()
            brain = handle.writer()
            await asyncio.to_thread(
                brain.register,
                b"Capitals.",
                service_module.RegistrationRequest(
                    media_type="text/plain", actor=actor
                ),
            )

    writer = asyncio.create_task(slow_write())
    await asyncio.sleep(0)
    discarding = asyncio.create_task(service.store.discard_if_empty(handle))
    await asyncio.sleep(0.05)
    gate.set()
    await writer
    assert await discarding is False
    assert service.store.exists(REF)
    assert handle.heads().head is not None


async def test_write_queued_behind_a_discard_is_refused(
    service: LifecycleService, actor
) -> None:
    service.create(REF, actor=actor)
    handle = service.handle(REF)
    assert await service.store.discard_if_empty(handle) is True
    with pytest.raises(errors.LifecycleError) as caught:
        await service.register_evidence(
            handle, b"late", actor=actor, media_type="text/plain"
        )
    assert caught.value.code == "not_found"
    assert not (handle.directory / LAYOUT_DIR).exists()


# --------------------------------------------------------------------------- #
# Governed check, stale read handles
# --------------------------------------------------------------------------- #


async def test_write_checks_governance_after_switching(
    service: LifecycleService, actor, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, source, _ = await _seed(service, actor)
    await service.create_branch(handle, "other", actor=actor)
    order: list[str] = []
    original_switch = LifecycleService._switch_to

    def switch(handle_, brain, branch):
        order.append("switch")
        return original_switch(handle_, brain, branch)

    def require(brain):
        order.append("require")

    monkeypatch.setattr(LifecycleService, "_switch_to", staticmethod(switch))
    monkeypatch.setattr(LifecycleService, "_require_writable", staticmethod(require))
    await service.register_evidence(
        handle, b"more", actor=actor, media_type="text/plain", branch="other"
    )
    assert order == ["switch", "require"]


async def test_convergence_is_read_at_the_live_head(
    service: LifecycleService, actor
) -> None:
    handle, source, _ = await _seed(service, actor)
    await service.create_branch(handle, "other", actor=actor)
    producer = {"kind": "model", "id": "openai/gpt-4.1"}
    await service.commit(
        handle,
        actor=actor,
        source=source,
        candidates=[_fact(source, "capital", "Paris.", subject="france")],
        producer=producer,
    )
    await service.commit(
        handle,
        actor=actor,
        source=source,
        candidates=[_fact(source, "capital", "Lyon.", subject="france")],
        producer=producer,
        branch="other",
    )
    # A read of main while "other" is current keeps a handle at main's head
    # whose head pointer says "other".
    service.list_blocks(handle, "semantic")
    job = service.converge(handle, actor=actor, source="other", strategy="merge")
    await job.task
    assert job.state is JobState.FAILED and job.error["code"] == "needs_human"
    view = service.convergence(handle)
    assert view["open"] is True and len(view["unresolved"]) == 1


# --------------------------------------------------------------------------- #
# If-Match on every write
# --------------------------------------------------------------------------- #

STALE = "sha256:" + "0" * 64


async def test_if_match_on_branch_writes(service: LifecycleService, actor) -> None:
    handle, _, head = await _seed(service, actor)
    with pytest.raises(errors.LifecycleError) as caught:
        await service.create_branch(handle, "x", actor=actor, if_match=STALE)
    assert caught.value.code == "not_fast_forward"
    await service.create_branch(handle, "x", actor=actor, if_match=head)
    with pytest.raises(errors.LifecycleError) as caught:
        await service.delete_branch(handle, "x", actor=actor, if_match=STALE)
    assert caught.value.code == "not_fast_forward"
    await service.delete_branch(handle, "x", actor=actor, if_match=f'"{head}"')


async def test_if_match_on_jobs(service: LifecycleService, actor) -> None:
    handle, _, head = await _seed(service, actor)
    stale_push = service.push(handle, "main", actor=actor, if_match=STALE)
    await stale_push.task
    assert stale_push.state is JobState.FAILED
    assert stale_push.error["code"] == "not_fast_forward"
    pushed = service.push(handle, "main", actor=actor, if_match=head)
    await pushed.task
    assert pushed.state is JobState.SUCCEEDED, pushed.error

    stale_pull = service.pull(handle, actor=actor, if_match=STALE)
    await stale_pull.task
    assert stale_pull.error["code"] == "not_fast_forward"
    pulled = service.pull(handle, actor=actor, if_match=head)
    await pulled.task
    assert pulled.state is JobState.SUCCEEDED, pulled.error

    await service.create_branch(handle, "other", actor=actor)
    stale = service.converge(handle, actor=actor, source="other", if_match=STALE)
    await stale.task
    assert stale.error["code"] == "not_fast_forward"
    joined = service.converge(handle, actor=actor, source="other", if_match=head)
    await joined.task
    assert joined.state is JobState.SUCCEEDED, joined.error


async def test_if_match_on_convergence_actions(
    service: LifecycleService, actor
) -> None:
    handle, _, _ = await _seed(service, actor)
    for call in (
        service.continue_convergence,
        service.abort_convergence,
        service.accept_removals,
    ):
        with pytest.raises(errors.LifecycleError) as caught:
            await call(handle, actor=actor, if_match=STALE)
        assert caught.value.code == "not_fast_forward"


# --------------------------------------------------------------------------- #
# Layout swap
# --------------------------------------------------------------------------- #


def test_replace_layout_restores_the_old_layout_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "brain" / "layout"
    target.mkdir(parents=True)
    (target / "old").write_text("old")
    incoming = tmp_path / "scratch" / "root"
    incoming.mkdir(parents=True)
    (incoming / "new").write_text("new")

    real_rename = Path.rename

    def failing_rename(self: Path, dest: Any) -> Path:
        if Path(dest) == target and self.name == "layout" and self != target:
            if self.parent != target.parent:
                raise OSError("disk full")
        return real_rename(self, dest)

    monkeypatch.setattr(Path, "rename", failing_rename)
    with pytest.raises(OSError):
        service_module._replace_layout(target, incoming)
    assert (target / "old").read_text() == "old"
    assert not target.with_name("layout.old").exists()
    assert [p.name for p in target.parent.iterdir()] == ["layout"]

    monkeypatch.setattr(Path, "rename", real_rename)
    incoming.mkdir(parents=True)  # the failed swap does not keep the upload
    (incoming / "new").write_text("new")
    service_module._replace_layout(target, incoming)
    assert (target / "new").read_text() == "new"
    assert sorted(p.name for p in target.parent.iterdir()) == ["layout"]
