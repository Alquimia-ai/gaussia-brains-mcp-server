"""Tests for short brain reference expansion."""

from __future__ import annotations

import pytest

from gaussia_brains_mcp_server.repo import (
    BrainRef,
    expand_brain_ref,
    expand_namespace,
    parse_brain_ref,
)


def test_parse_short_brain_ref() -> None:
    ref = parse_brain_ref("brains/my-brain:latest")
    assert ref == BrainRef(
        registry="registry.alquimia.ai",
        namespace="brains",
        name="my-brain",
        tag="latest",
    )
    assert ref.reference == "registry.alquimia.ai/brains/my-brain"
    assert ref.target == "registry.alquimia.ai/brains/my-brain:latest"


def test_parse_bare_name_uses_default_namespace() -> None:
    ref = parse_brain_ref("my-brain")
    assert ref.namespace == "brains"
    assert ref.name == "my-brain"
    assert ref.tag == "latest"


def test_parse_full_reference_unchanged() -> None:
    ref = parse_brain_ref("registry.example.com/acme/handbook:main")
    assert ref == BrainRef(
        registry="registry.example.com",
        namespace="acme",
        name="handbook",
        tag="main",
    )


def test_parse_with_digest() -> None:
    digest = "sha256:" + "a" * 64
    ref = parse_brain_ref(f"brains/my-brain@{digest}")
    assert ref.digest == digest
    assert ref.tag == "latest"
    assert ref.target == f"registry.alquimia.ai/brains/my-brain@{digest}"


def test_parse_registry_without_namespace() -> None:
    ref = parse_brain_ref("registry.example.com/my-brain:v1")
    assert ref.registry == "registry.example.com"
    assert ref.namespace == "brains"
    assert ref.name == "my-brain"


def test_expand_brain_ref_string() -> None:
    assert (
        expand_brain_ref("brains/my-brain:latest")
        == "registry.alquimia.ai/brains/my-brain:latest"
    )


def test_expand_brain_ref_with_custom_defaults() -> None:
    assert (
        expand_brain_ref(
            "my-brain:v2",
            registry_default="ghcr.io",
            namespace_default="knowledge",
        )
        == "ghcr.io/knowledge/my-brain:v2"
    )


def test_expand_namespace_short_form() -> None:
    assert expand_namespace("brains") == "registry.alquimia.ai/brains"


def test_expand_namespace_full_form_unchanged() -> None:
    assert (
        expand_namespace("registry.example.com/acme")
        == "registry.example.com/acme"
    )


def test_invalid_reference_raises() -> None:
    with pytest.raises(ValueError):
        parse_brain_ref("")
    with pytest.raises(ValueError):
        parse_brain_ref("brains/name@notadigest")
