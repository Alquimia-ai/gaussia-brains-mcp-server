"""Stable error codes of the lifecycle REST surface.

Each code maps one-to-one to an outcome a REST client (Ledger) acts on, so a
client branches on ``code`` and never on message text. Every error body is::

    {"code": "<code>", "message": "...", "detail": {...}}

====================  ======  ===============================================
code                  status  meaning / what the client does
====================  ======  ===============================================
usage                 400     asked wrong: bad name, bad input, missing
                              strategy, a brain or branch that already
                              exists. Fix the request.
not_found             404     the brain, branch, block, job or remote
                              reference does not exist.
refused_protocol      422     the protocol refused the operation: append-only
                              violation, unmerged branch, rollback, a
                              snapshot this brain does not hold.
refused_policy        403     a policy refused it: a governed brain (served
                              read-only), a source outside the allow-list, an
                              authenticity verdict, a read-only replica.
rejected_candidates   422     a commit whose candidates did not all validate;
                              nothing was written. ``detail.report`` has
                              every verdict.
not_fast_forward      409     ``If-Match`` named a head that is no longer the
                              head, the remote moved on, or another write to
                              the brain is in progress. Re-read, then
                              converge or retry.
registry_unavailable  502     the OCI registry could not be reached or read.
                              Retry later.
needs_human           409     a convergence is open, or stopped to ask.
                              Resolve it via ``/convergence``.
internal              500     anything else (not a client outcome).
====================  ======  ===============================================

Codes are part of the semver contract: they are appended, never reassigned.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

USAGE = "usage"
NOT_FOUND = "not_found"
REFUSED_PROTOCOL = "refused_protocol"
REFUSED_POLICY = "refused_policy"
REJECTED_CANDIDATES = "rejected_candidates"
NOT_FAST_FORWARD = "not_fast_forward"
REGISTRY_UNAVAILABLE = "registry_unavailable"
NEEDS_HUMAN = "needs_human"
INTERNAL = "internal"

STATUS: dict[str, int] = {
    USAGE: 400,
    NOT_FOUND: 404,
    REFUSED_PROTOCOL: 422,
    REFUSED_POLICY: 403,
    REJECTED_CANDIDATES: 422,
    NOT_FAST_FORWARD: 409,
    REGISTRY_UNAVAILABLE: 502,
    NEEDS_HUMAN: 409,
    INTERNAL: 500,
}

ALL_CODES = tuple(STATUS)


class LifecycleError(Exception):
    """An error this layer raises with its stable code already decided."""

    def __init__(
        self, code: str, message: str, *, detail: dict[str, Any] | None = None
    ) -> None:
        if code not in STATUS:
            raise ValueError(f"unknown lifecycle error code {code!r}")
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail or {}

    @property
    def status(self) -> int:
        return STATUS[self.code]


@dataclass(frozen=True)
class ErrorInfo:
    code: str
    message: str
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def status(self) -> int:
        return STATUS[self.code]

    def body(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "detail": self.detail}


#: Exception class names (walked along the MRO, nearest first) to codes. Names
#: rather than classes so that this table does not import half the SDK, and so
#: that service exceptions defined elsewhere map without a circular import.
_BY_NAME: dict[str, str] = {
    # -- service ---------------------------------------------------------------
    "InvalidTokenError": REFUSED_POLICY,
    "SourceNotAllowedError": REFUSED_POLICY,
    "UntrustedSignatureError": REFUSED_POLICY,
    "BrainUnavailableError": REGISTRY_UNAVAILABLE,
    "BrainManifestRejected": REFUSED_PROTOCOL,
    "EmbeddingsError": INTERNAL,
    # -- pyboltzmann: concurrency and history ----------------------------------
    "LostPublishError": NOT_FAST_FORWARD,
    "DivergenceError": NOT_FAST_FORWARD,
    "RollbackError": REFUSED_PROTOCOL,
    "ReconciliationHaltedError": NEEDS_HUMAN,
    "ReconciliationBlockedError": NEEDS_HUMAN,
    "ResolutionRefusedError": USAGE,
    "NoCommonAncestorError": REFUSED_PROTOCOL,
    "MultipleMergeBasesError": REFUSED_PROTOCOL,
    "GovernanceConflictError": REFUSED_POLICY,
    "ReconciliationError": REFUSED_PROTOCOL,
    # -- pyboltzmann: branches -------------------------------------------------
    "BranchNotFoundError": NOT_FOUND,
    "BranchExistsError": USAGE,
    "InvalidBranchNameError": USAGE,
    "UnmergedBranchError": REFUSED_PROTOCOL,
    "BranchError": USAGE,
    # -- pyboltzmann: lookup ---------------------------------------------------
    "ReferenceNotFoundError": NOT_FOUND,
    "BlockNotFoundError": NOT_FOUND,
    "MembershipError": NOT_FOUND,
    "BlockTombstonedError": REFUSED_PROTOCOL,
    "MemoryTypeError": USAGE,
    "QueryError": USAGE,
    "ActorIdError": REFUSED_POLICY,
    "DigestFormatError": USAGE,
    "DigestKindError": USAGE,
    # -- pyboltzmann: policy ---------------------------------------------------
    "AuthenticityError": REFUSED_POLICY,
    "RetentionPolicyError": REFUSED_POLICY,
    # -- pyboltzmann: everything else the protocol refuses ---------------------
    "AppendOnlyViolationError": REFUSED_PROTOCOL,
    "RemovalInvariantError": REFUSED_PROTOCOL,
    "SnapshotError": REFUSED_PROTOCOL,
    "ValidationError": REFUSED_PROTOCOL,
    "CommitError": REFUSED_PROTOCOL,
    "BlockSchemaError": USAGE,
    "DistributionError": REGISTRY_UNAVAILABLE,
    "ProtocolError": REFUSED_PROTOCOL,
    "BoltzmannError": REFUSED_PROTOCOL,
    # -- plain Python ----------------------------------------------------------
    "ValueError": USAGE,
}


def describe(error: BaseException) -> ErrorInfo:
    """The stable code, message and detail for *error*."""
    if isinstance(error, LifecycleError):
        return ErrorInfo(error.code, error.message, dict(error.detail))
    for cls in type(error).__mro__:
        code = _BY_NAME.get(cls.__name__)
        if code is not None:
            message = str(error) or cls.__name__
            return ErrorInfo(code, message, {"error": type(error).__name__})
    return ErrorInfo(INTERNAL, "unexpected error", {"error": type(error).__name__})
