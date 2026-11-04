"""Stable error codes for the MCP and REST surfaces.

Codes are part of the public contract (docs/proposal.md §4.3): clients branch
on ``code``, never on message text. Mapping walks the exception's MRO, so a
subclass maps like its nearest known ancestor.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BrainErrorInfo:
    code: str
    http_status: int
    hint: str

    @property
    def message(self) -> str:
        return self.hint


_BY_NAME: dict[str, BrainErrorInfo] = {
    # Service
    "InvalidTokenError": BrainErrorInfo(
        "INVALID_TOKEN", 401, "The bearer token was not accepted."
    ),
    "BrainUnavailableError": BrainErrorInfo(
        "BRAIN_UNAVAILABLE", 503, "The brain could not be opened; retry later."
    ),
    "SourceNotAllowedError": BrainErrorInfo(
        "SOURCE_NOT_ALLOWED",
        403,
        "The brain's registry or namespace is not served by this server.",
    ),
    "UntrustedSignatureError": BrainErrorInfo(
        "UNTRUSTED_SIGNATURE",
        403,
        "The brain is not signed by a signer this server trusts.",
    ),
    "BrainManifestRejected": BrainErrorInfo(
        "MANIFEST_REJECTED",
        422,
        "The reference is not a brain this service can install.",
    ),
    # pyboltzmann
    "ReferenceNotFoundError": BrainErrorInfo(
        "REFERENCE_NOT_FOUND", 404, "The requested brain reference does not exist."
    ),
    "BranchNotFoundError": BrainErrorInfo(
        "BRANCH_NOT_FOUND", 404, "The requested branch was not found."
    ),
    "AuthenticityError": BrainErrorInfo(
        "AUTHENTICITY_REJECTED",
        409,
        "The brain's head is not authored as the verification policy requires.",
    ),
    "RollbackError": BrainErrorInfo(
        "ROLLBACK_REFUSED", 409, "The registry serves an older snapshot than held."
    ),
    "BlockNotFoundError": BrainErrorInfo(
        "BLOCK_NOT_FOUND", 404, "The requested block was not found."
    ),
    "BlockTombstonedError": BrainErrorInfo(
        "BLOCK_TOMBSTONED", 410, "The requested block has been removed."
    ),
    "MemoryTypeError": BrainErrorInfo(
        "MEMORY_TYPE_INVALID",
        400,
        "The requested memory type is not part of this brain.",
    ),
    "QueryError": BrainErrorInfo(
        "QUERY_INVALID", 400, "The query could not be executed."
    ),
    "DistributionError": BrainErrorInfo(
        "REGISTRY_ERROR", 502, "The OCI registry could not be read."
    ),
}

_INTERNAL = BrainErrorInfo("INTERNAL", 500, "Unexpected error.")


def describe_brain_error(error: BaseException) -> BrainErrorInfo:
    """Map *error* to its stable description."""
    for cls in type(error).__mro__:
        info = _BY_NAME.get(cls.__name__)
        if info is not None:
            return info
    return _INTERNAL
