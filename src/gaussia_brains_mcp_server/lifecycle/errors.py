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
unauthenticated       401     no bearer token, or one that did not verify.
                              Get a (new) token; ``WWW-Authenticate`` says
                              where.
====================  ======  ===============================================

Codes are part of the semver contract: they are appended, never reassigned.

Authorization failures are not a separate code: a token that verified but
lacks a scope is ``refused_policy`` (403), the same outcome as any other
policy refusal. Authentication failures need their own code because their
status (401) differs and the client's remedy (re-authenticate) is not one of
the others; ``unauthenticated`` was appended for them.

A few outcomes keep their code but carry another status: a request body
larger than the service accepts is ``usage`` with ``413``.

Only exceptions defined by this service or by pyboltzmann are mapped by
:func:`describe`; anything else (``ValueError``, ``KeyError``, pydantic's
``ValidationError`` from internal parsing, ...) is ``internal``, so that a
bug never reads as the client's fault. Request parsing turns its own errors
into ``usage`` explicitly.
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
UNAUTHENTICATED = "unauthenticated"

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
    UNAUTHENTICATED: 401,
}

ALL_CODES = tuple(STATUS)


class LifecycleError(Exception):
    """An error this layer raises with its stable code already decided."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        detail: dict[str, Any] | None = None,
        status: int | None = None,
    ) -> None:
        if code not in STATUS:
            raise ValueError(f"unknown lifecycle error code {code!r}")
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail or {}
        self._status = status

    @property
    def status(self) -> int:
        return self._status or STATUS[self.code]


@dataclass(frozen=True)
class ErrorInfo:
    code: str
    message: str
    detail: dict[str, Any] = field(default_factory=dict)
    status_override: int | None = None

    @property
    def status(self) -> int:
        return self.status_override or STATUS[self.code]

    def body(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "detail": self.detail}


#: Exception class names (walked along the MRO, nearest first) to codes. Names
#: rather than classes so that this table does not import half the SDK, and so
#: that service exceptions defined elsewhere map without a circular import. A
#: name only counts for a class defined in one of :data:`_MAPPED_PACKAGES`, so
#: pydantic's ``ValidationError`` is not mistaken for pyboltzmann's.
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
}

#: Top-level packages whose exception classes :data:`_BY_NAME` describes.
_MAPPED_PACKAGES = frozenset({"boltzmann", "gaussia_brains_mcp_server"})


def describe(error: BaseException) -> ErrorInfo:
    """The stable code, message and detail for *error*."""
    if isinstance(error, LifecycleError):
        return ErrorInfo(error.code, error.message, dict(error.detail), error._status)
    for cls in type(error).__mro__:
        if cls.__module__.split(".", 1)[0] not in _MAPPED_PACKAGES:
            continue
        code = _BY_NAME.get(cls.__name__)
        if code is not None:
            message = str(error) or cls.__name__
            return ErrorInfo(code, message, {"error": type(error).__name__})
    if isinstance(error, ValueError) and _raised_in_boltzmann(error):
        # pyboltzmann validates the records a client hands it (candidate
        # blocks, timestamps, provenance) with plain ValueErrors: the input is
        # wrong, not the service. A ValueError raised by our own code is a bug.
        return ErrorInfo(USAGE, str(error) or "invalid input", {"error": "ValueError"})
    return ErrorInfo(INTERNAL, "unexpected error", {"error": type(error).__name__})


def _raised_in_boltzmann(error: BaseException) -> bool:
    """Whether the innermost frame of *error*'s traceback is pyboltzmann code."""
    tb = error.__traceback__
    if tb is None:
        return False
    while tb.tb_next is not None:
        tb = tb.tb_next
    module = tb.tb_frame.f_globals.get("__name__", "")
    return module.split(".", 1)[0] == "boltzmann"
