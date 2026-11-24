"""Units of the lifecycle layer: error codes, actor identity, jobs."""

from __future__ import annotations

import asyncio

import pytest
from boltzmann.exceptions import (
    BranchExistsError,
    BranchNotFoundError,
    DistributionError,
    DivergenceError,
    LostPublishError,
    ReconciliationHaltedError,
    RollbackError,
    UnmergedBranchError,
)

from conftest import ISSUER
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.config import Settings
from gaussia_brains_mcp_server.lifecycle import errors
from gaussia_brains_mcp_server.lifecycle.identity import actor_for
from gaussia_brains_mcp_server.lifecycle.jobs import JobRunner, JobState
from gaussia_brains_mcp_server.lifecycle.service import (
    LifecycleService,
    normalize_if_match,
)


@pytest.mark.parametrize(
    ("error", "code", "status"),
    [
        (LostPublishError("x", published="a", observed="b"), "not_fast_forward", 409),
        (DivergenceError("x"), "not_fast_forward", 409),
        (ReconciliationHaltedError("x"), "needs_human", 409),
        (BranchNotFoundError("x"), "not_found", 404),
        (BranchExistsError("x"), "usage", 400),
        (UnmergedBranchError("x"), "refused_protocol", 422),
        (RollbackError("x"), "refused_protocol", 422),
        (DistributionError("x"), "registry_unavailable", 502),
        (ValueError("x"), "internal", 500),
        (RuntimeError("x"), "internal", 500),
    ],
)
def test_error_codes(error: Exception, code: str, status: int) -> None:
    info = errors.describe(error)
    assert (info.code, info.status) == (code, status)
    assert set(info.body()) == {"code", "message", "detail"}


def test_codes_map_one_to_one_to_ledger_outcomes() -> None:
    assert set(errors.ALL_CODES) == {
        "usage",
        "not_found",
        "refused_protocol",
        "refused_policy",
        "rejected_candidates",
        "not_fast_forward",
        "registry_unavailable",
        "needs_human",
        "internal",
        "unauthenticated",
    }
    with pytest.raises(ValueError):
        errors.LifecycleError("nope", "x")


def _principal(**claims) -> Principal:
    return Principal(subject="2F6C-Sub", issuer=ISSUER, claims=claims)


def test_actor_prefers_the_address_claim() -> None:
    actor = actor_for(_principal(email="Ana@Example.com", name="Ana"), Settings())
    assert actor.id == "ana@example.com"
    assert actor.kind.value == "human"
    assert actor.name == "Ana"


def test_actor_falls_back_to_a_namespaced_subject() -> None:
    assert actor_for(_principal(), Settings()).id == "idp.example.com/2f6c-sub"
    custom = Settings(actor_namespace="acme")
    assert actor_for(_principal(email="not-an-address"), custom).id == "acme/2f6c-sub"


def test_actor_refused_when_nothing_is_usable() -> None:
    principal = Principal(subject="has space", issuer=ISSUER, claims={})
    with pytest.raises(errors.LifecycleError) as caught:
        actor_for(principal, Settings())
    assert caught.value.code == "refused_policy"


def test_if_match_normalization() -> None:
    assert normalize_if_match('W/"sha256:abc"') == "sha256:abc"
    assert normalize_if_match(' "sha256:abc" ') == "sha256:abc"
    assert normalize_if_match(None) is None


def test_repository_strips_tag_and_refuses_digests() -> None:
    service = LifecycleService.__new__(LifecycleService)
    service.settings = Settings()
    assert service.repository("brains/handbook:stable") == (
        "registry.alquimia.ai/brains/handbook",
        "stable",
    )
    assert service.repository("localhost:5000/acme/x") == (
        "localhost:5000/acme/x",
        None,
    )
    with pytest.raises(errors.LifecycleError):
        service.repository("brains/x@sha256:" + "a" * 64)


async def test_job_cancel_before_and_after_commit_point() -> None:
    runner = JobRunner()
    gate = asyncio.Event()

    async def slow(ctx):
        ctx.stage("uploading")
        await gate.wait()
        ctx.commit_point("writing manifest")
        await asyncio.sleep(0)
        return {"ok": True}

    first = runner.submit("push", brain="b", actor="a", body=slow)
    await asyncio.sleep(0)
    assert first.cancellable
    runner.cancel(first.id)
    await asyncio.gather(first.task, return_exceptions=True)
    assert first.state is JobState.CANCELLED

    second = runner.submit("push", brain="b", actor="a", body=slow)
    await asyncio.sleep(0)
    gate.set()
    for _ in range(5):
        await asyncio.sleep(0)
        if not second.cancellable:
            break
    with pytest.raises(errors.LifecycleError) as caught:
        runner.cancel(second.id)
    assert caught.value.code == "usage"
    await second.task
    assert second.state is JobState.SUCCEEDED
    assert second.view()["result"] == {"ok": True}


async def test_failed_job_reports_its_code() -> None:
    runner = JobRunner()

    async def boom(_ctx):
        raise DivergenceError("remote moved")

    job = runner.submit("push", brain="b", actor="a", body=boom)
    await job.task
    assert job.state is JobState.FAILED
    assert job.error["code"] == "not_fast_forward"


async def test_retention_is_bounded() -> None:
    runner = JobRunner(max_retained=2)

    async def ok(_ctx):
        return {}

    jobs = [runner.submit("pull", brain="b", actor="a", body=ok) for _ in range(3)]
    await asyncio.gather(*(j.task for j in jobs))
    runner.submit("pull", brain="b", actor="a", body=ok)
    assert len(runner.list()) <= 3


async def test_cancel_before_the_job_starts_is_recorded_and_cleaned_up() -> None:
    runner = JobRunner()
    cleaned: list[bool] = []
    ran: list[bool] = []

    async def body(_ctx):
        ran.append(True)
        return {}

    job = runner.submit(
        "import", brain="b", actor="a", body=body, cleanup=lambda: cleaned.append(True)
    )
    assert job.state is JobState.QUEUED
    runner.cancel(job.id)  # before the task ever ran
    await job.task
    assert job.state is JobState.CANCELLED
    assert job.finished_at is not None
    assert ran == [] and cleaned == [True]


async def test_cleanup_does_not_run_on_success() -> None:
    runner = JobRunner()
    cleaned: list[bool] = []

    async def body(_ctx):
        return {}

    job = runner.submit(
        "pull", brain="b", actor="a", body=body, cleanup=lambda: cleaned.append(True)
    )
    await job.task
    assert job.state is JobState.SUCCEEDED and cleaned == []
