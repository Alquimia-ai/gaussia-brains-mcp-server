"""Publisher-signature policy: cosign is stubbed, the allow-list logic is real."""

from __future__ import annotations

import json
import subprocess

import pytest
from pydantic import ValidationError

from gaussia_brains_mcp_server.config import Settings, TrustedSigner
from gaussia_brains_mcp_server.signatures import (
    SignatureVerifier,
    UntrustedSignatureError,
    parse_cosign_output,
)

TARGET = "registry.example.com/brains/handbook@sha256:" + "a" * 64
GITHUB = "https://token.actions.githubusercontent.com"
WORKFLOW = (
    "https://github.com/acme/brains/.github/workflows/publish.yml@refs/heads/main"
)


def cosign_json(*identities: tuple[str, str]) -> str:
    return json.dumps(
        [
            {
                "critical": {
                    "identity": {
                        "docker-reference": "registry.example.com/brains/handbook"
                    }
                },
                "optional": {"Issuer": issuer, "Subject": subject},
            }
            for issuer, subject in identities
        ]
    )


class FakeCosign:
    """Answers `cosign verify` like the real CLI: exit code + JSON on stdout."""

    def __init__(self, keyless: str | None = None, key_ok: bool = False) -> None:
        self.keyless = keyless
        self.key_ok = key_ok
        self.calls: list[list[str]] = []

    def __call__(self, argv, **_kw) -> subprocess.CompletedProcess:
        self.calls.append(argv)
        if "--key" in argv:
            ok = self.key_ok
            return subprocess.CompletedProcess(
                argv,
                0 if ok else 1,
                "[]" if ok else "",
                "" if ok else "Error: no matching signatures",
            )
        if self.keyless is None:
            return subprocess.CompletedProcess(
                argv, 1, "", "Error: no matching signatures"
            )
        return subprocess.CompletedProcess(argv, 0, self.keyless, "")


def verifier(cosign: FakeCosign, **settings) -> SignatureVerifier:
    return SignatureVerifier(
        Settings(**settings), run=cosign, which=lambda _: "/usr/bin/cosign"
    )


ALICE = TrustedSigner(issuer="https://accounts.google.com", subject="alice@acme.com")
CI = TrustedSigner(issuer=GITHUB, subject_regex=r"https://github\.com/acme/.+")


def test_parse_cosign_output_reads_identities() -> None:
    out = cosign_json(
        ("https://accounts.google.com", "alice@acme.com"), (GITHUB, WORKFLOW)
    )
    signers = parse_cosign_output(out)
    assert [(s.issuer, s.subject) for s in signers] == [
        ("https://accounts.google.com", "alice@acme.com"),
        (GITHUB, WORKFLOW),
    ]


def test_parse_cosign_output_falls_back_to_fulcio_oid_and_lines() -> None:
    line = json.dumps(
        {"optional": {"1.3.6.1.4.1.57264.1.8": GITHUB, "Subject": WORKFLOW}}
    )
    assert parse_cosign_output(line + "\n" + line)[0].issuer == GITHUB


def test_keyless_signer_on_the_allow_list_is_trusted() -> None:
    v = verifier(
        FakeCosign(cosign_json((GITHUB, WORKFLOW))),
        signature_policy="required",
        trusted_signers=[ALICE, CI],
    )
    report = v.gate(TARGET)
    assert report is not None and report.trusted
    assert report.signers[0].subject == WORKFLOW


def test_verification_accepts_any_identity_then_matches_pairs() -> None:
    cosign = FakeCosign(cosign_json((GITHUB, WORKFLOW)))
    verifier(cosign, signature_policy="required", trusted_signers=[CI]).inspect(TARGET)
    argv = cosign.calls[0]
    assert argv[argv.index("--certificate-identity-regexp") + 1] == ".+"
    assert argv[-1] == TARGET


def test_identity_from_the_wrong_issuer_is_not_trusted() -> None:
    """alice@acme.com is trusted via Google, not via another issuer."""
    v = verifier(
        FakeCosign(cosign_json(("https://evil-idp.example.com", "alice@acme.com"))),
        signature_policy="required",
        trusted_signers=[ALICE],
    )
    with pytest.raises(UntrustedSignatureError, match="none trusted"):
        v.gate(TARGET)


def test_subject_regex_is_a_full_match() -> None:
    v = verifier(
        FakeCosign(cosign_json((GITHUB, "https://github.com/acme-evil/x"))),
        signature_policy="required",
        trusted_signers=[
            TrustedSigner(issuer=GITHUB, subject_regex=r"https://github\.com/acme")
        ],
    )
    with pytest.raises(UntrustedSignatureError):
        v.gate(TARGET)


def test_unsigned_brain_under_warn_is_served_with_report() -> None:
    v = verifier(FakeCosign(), signature_policy="warn", trusted_signers=[ALICE])
    report = v.gate(TARGET)
    assert report is not None and not report.trusted
    assert "no matching signatures" in report.detail


def test_policy_off_never_runs_cosign() -> None:
    cosign = FakeCosign()
    assert verifier(cosign).gate(TARGET) is None
    assert cosign.calls == []


def test_configured_key_is_an_alternative_to_keyless() -> None:
    v = verifier(
        FakeCosign(key_ok=True),
        signature_policy="required",
        cosign_key="k8s://ns/brains",
    )
    report = v.gate(TARGET)
    assert report is not None and report.trusted and report.detail == ""


def test_reports_are_cached_per_digest() -> None:
    cosign = FakeCosign(cosign_json((GITHUB, WORKFLOW)))
    v = verifier(cosign, signature_policy="required", trusted_signers=[CI])
    v.gate(TARGET)
    v.gate(TARGET)
    assert len(cosign.calls) == 1


def test_tags_are_refused() -> None:
    with pytest.raises(ValueError, match="digest"):
        verifier(FakeCosign()).inspect("registry.example.com/brains/handbook:main")


def test_missing_cosign_is_reported_not_crashed() -> None:
    v = SignatureVerifier(Settings(), run=FakeCosign(), which=lambda _: None)
    report = v.inspect(TARGET)
    assert not report.checked and not report.trusted


def test_enforcing_policy_needs_signers_or_key() -> None:
    from gaussia_brains_mcp_server.config import SignaturePolicy

    with pytest.raises(ValidationError, match="TRUSTED_SIGNERS"):
        Settings(signature_policy=SignaturePolicy.REQUIRED)


def test_trusted_signer_needs_exactly_one_subject_form() -> None:
    with pytest.raises(ValidationError):
        TrustedSigner(issuer=GITHUB)
    with pytest.raises(ValidationError):
        TrustedSigner(issuer=GITHUB, subject="a", subject_regex="b")


def test_trusted_signers_parse_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("GBMS_SIGNATURE_POLICY", "required")
    monkeypatch.setenv(
        "GBMS_TRUSTED_SIGNERS",
        json.dumps(
            [{"issuer": GITHUB, "subject_regex": "https://github\\.com/acme/.+"}]
        ),
    )
    assert Settings().trusted_signers[0].matches(GITHUB, WORKFLOW)
