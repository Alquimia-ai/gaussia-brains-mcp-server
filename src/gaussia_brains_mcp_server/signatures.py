"""Publisher signatures: who signed a brain, and whether this server trusts them.

Brains are published headless: whoever publishes one signs it, ideally keyless
with Sigstore (cosign + Fulcio + Rekor), so the signature is bound to the
publisher's OIDC identity -- an email, or a CI workflow URI -- instead of a
key someone has to distribute.

Verification is split in two on purpose:

1. ``cosign verify`` does the cryptography: the certificate chains to the
   Sigstore root, the signature is in the transparency log, and it covers the
   manifest digest. It is asked to accept *any* identity
   (``--certificate-identity-regexp .+``) and to report what it verified.
2. This module matches each verified ``(issuer, subject)`` pair against
   ``GBMS_TRUSTED_SIGNERS``. Matching pairs here, rather than handing cosign
   one alternation regex per field, avoids accepting identity A from issuer B.

The same report feeds discovery (show who signed each brain) and the pull
gate (``GBMS_SIGNATURE_POLICY``: off / warn / required).

Always verify ``repo@sha256:...``, never a tag: a tag can move between the
check and the pull.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from boltzmann.exceptions import DistributionError
from loguru import logger

from gaussia_brains_mcp_server.config import Settings, SignaturePolicy

# Fulcio certificate extensions carrying the OIDC issuer (v1 raw, v2 DER-UTF8).
_ISSUER_OIDS = ("1.3.6.1.4.1.57264.1.8", "1.3.6.1.4.1.57264.1.1")


class UntrustedSignatureError(DistributionError):
    """``GBMS_SIGNATURE_POLICY=required`` and no trusted signer signed the brain."""


@dataclass(frozen=True)
class Signer:
    """One cryptographically verified signature on a brain."""

    method: Literal["keyless", "key"]
    issuer: str | None = None
    subject: str | None = None

    def describe(self) -> str:
        if self.method == "key":
            return "configured key"
        return f"{self.subject} ({self.issuer})"


@dataclass(frozen=True)
class SignatureReport:
    """What is known about the signatures on one manifest digest."""

    target: str
    #: Verified signatures, whether or not their signer is trusted.
    signers: tuple[Signer, ...] = ()
    #: At least one verified signer is in the allow-list (or the key verified).
    trusted: bool = False
    #: False when no check could be made (cosign missing, policy off).
    checked: bool = True
    detail: str = ""
    checked_at: float = field(default=0.0, compare=False)

    def summary(self) -> dict[str, Any]:
        return {
            "checked": self.checked,
            "trusted": self.trusted,
            "signers": [
                {"method": s.method, "issuer": s.issuer, "subject": s.subject}
                for s in self.signers
            ],
            "detail": self.detail or None,
        }


def parse_cosign_output(stdout: str) -> list[Signer]:
    """Extract verified keyless identities from ``cosign verify -o json``.

    cosign prints a JSON array (older builds: one object per line). The
    identity of each verified signature is in ``optional.Subject`` and
    ``optional.Issuer``, or in the Fulcio extension OIDs.
    """
    text = stdout.strip()
    if not text:
        return []
    try:
        loaded = json.loads(text)
        items = loaded if isinstance(loaded, list) else [loaded]
    except json.JSONDecodeError:
        items = []
        for line in text.splitlines():
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    signers: list[Signer] = []
    for item in items:
        optional = item.get("optional") if isinstance(item, dict) else None
        if not isinstance(optional, dict):
            continue
        issuer = optional.get("Issuer")
        if not issuer:
            issuer = next((optional[o] for o in _ISSUER_OIDS if optional.get(o)), None)
        subject = optional.get("Subject")
        if issuer or subject:
            signers.append(Signer("keyless", str(issuer) if issuer else None, subject))
    return list(dict.fromkeys(signers))  # de-duplicate, keep order


class SignatureVerifier:
    """Runs cosign, evaluates the allow-list, caches reports per digest.

    A report on a digest is cached for ``ttl`` seconds: a digest's content
    cannot change, but signatures can be *added* beside it, so an untrusted
    verdict must be allowed to expire.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        run: Callable[..., subprocess.CompletedProcess] = subprocess.run,
        which: Callable[[str], str | None] = shutil.which,
        ttl: float = 600.0,
        max_entries: int = 1024,
    ) -> None:
        self.settings = settings
        self._run = run
        self._which = which
        self._ttl = ttl
        self._max = max_entries
        self._cache: OrderedDict[str, SignatureReport] = OrderedDict()
        self._lock = threading.Lock()

    # -- cosign ---------------------------------------------------------------- #

    def _transport_flags(self) -> list[str]:
        flags: list[str] = []
        if self.settings.oras_plain_http:
            flags.append("--allow-http-registry=true")
        if self.settings.oras_insecure:
            flags.append("--allow-insecure-registry=true")
        return flags

    def keyless_argv(self, target: str) -> list[str]:
        return [
            self.settings.cosign_bin,
            "verify",
            "--certificate-identity-regexp",
            ".+",
            "--certificate-oidc-issuer-regexp",
            ".+",
            "--output",
            "json",
            *self._transport_flags(),
            target,
        ]

    def key_argv(self, target: str) -> list[str]:
        return [
            self.settings.cosign_bin,
            "verify",
            "--key",
            self.settings.cosign_key or "",
            "--output",
            "json",
            *self._transport_flags(),
            target,
        ]

    def _cosign(self, argv: list[str]) -> tuple[bool, str, str]:
        try:
            done = self._run(
                argv,
                capture_output=True,
                text=True,
                timeout=self.settings.oras_timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            return False, "", "cosign verify timed out"
        return done.returncode == 0, done.stdout or "", (done.stderr or "").strip()

    # -- public API -------------------------------------------------------------- #

    def inspect(self, target: str, *, use_cache: bool = True) -> SignatureReport:
        """Verify *target* (``repo@sha256:...``) and evaluate the allow-list."""
        if "@" not in target:
            raise ValueError(f"verify a digest reference, not a tag: {target!r}")
        if use_cache:
            cached = self._cached(target)
            if cached is not None:
                return cached
        report = self._inspect(target)
        self._store(report)
        return report

    def _inspect(self, target: str) -> SignatureReport:
        if self._which(self.settings.cosign_bin) is None:
            return SignatureReport(
                target,
                checked=False,
                detail=f"{self.settings.cosign_bin!r} is not installed",
                checked_at=time.monotonic(),
            )
        signers: list[Signer] = []
        details: list[str] = []

        ok, stdout, stderr = self._cosign(self.keyless_argv(target))
        if ok:
            signers += parse_cosign_output(stdout)
        elif stderr:
            details.append(stderr.splitlines()[-1])

        if self.settings.cosign_key:
            ok, _, stderr = self._cosign(self.key_argv(target))
            if ok:
                signers.append(Signer("key"))
            elif stderr:
                details.append(stderr.splitlines()[-1])

        trusted = any(
            s.method == "key"
            or any(
                t.matches(s.issuer, s.subject) for t in self.settings.trusted_signers
            )
            for s in signers
        )
        if trusted:
            details = []
        elif signers:
            details.append(
                "signed by "
                + ", ".join(s.describe() for s in signers)
                + ", none trusted"
            )
        if not signers and not details:
            details.append("no signatures found")
        return SignatureReport(
            target,
            signers=tuple(signers),
            trusted=trusted,
            detail="; ".join(details),
            checked_at=time.monotonic(),
        )

    def gate(self, target: str) -> SignatureReport | None:
        """Apply ``GBMS_SIGNATURE_POLICY`` to *target* before it is installed."""
        policy = self.settings.signature_policy
        if policy is SignaturePolicy.OFF:
            return None
        report = self.inspect(target)
        if report.trusted:
            logger.info(
                "Brain {} signed by {}",
                target,
                ", ".join(s.describe() for s in report.signers),
            )
            return report
        message = f"brain {target} is not signed by a trusted signer: {report.detail}"
        if policy is SignaturePolicy.REQUIRED:
            raise UntrustedSignatureError(message)
        logger.warning("{} (policy=warn, serving anyway)", message)
        return report

    def invalidate(self, target: str | None = None) -> None:
        with self._lock:
            if target is None:
                self._cache.clear()
            else:
                self._cache.pop(target, None)

    # -- cache ---------------------------------------------------------------- #

    def _cached(self, target: str) -> SignatureReport | None:
        with self._lock:
            report = self._cache.get(target)
            if report is None:
                return None
            if time.monotonic() - report.checked_at >= self._ttl:
                self._cache.pop(target, None)
                return None
            self._cache.move_to_end(target)
            return report

    def _store(self, report: SignatureReport) -> None:
        with self._lock:
            self._cache[report.target] = report
            self._cache.move_to_end(report.target)
            while len(self._cache) > self._max:
                self._cache.popitem(last=False)


_verifier: SignatureVerifier | None = None
_verifier_lock = threading.Lock()


def get_signature_verifier(settings: Settings) -> SignatureVerifier:
    """The process-wide verifier (its cache outlives individual clients)."""
    global _verifier
    with _verifier_lock:
        if _verifier is None or _verifier.settings is not settings:
            _verifier = SignatureVerifier(settings)
        return _verifier
