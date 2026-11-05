"""Brain reference parsing and short-form expansion.

Mirrors the minimal URI handling used by ``alquimia-core`` so callers can
address brains as ``brains/name:tag`` and have it expand to the configured
registry and namespace.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: OCI tag pattern: must match [a-zA-Z0-9_.-]{1,128}
OCI_TAG_PATTERN = re.compile(r"^[a-zA-Z0-9_.\-]{1,128}$")


def _looks_like_registry(part: str) -> bool:
    """A path component is a registry host when it has a dot, colon, or is localhost."""
    return "." in part or ":" in part or part == "localhost"


@dataclass(frozen=True)
class BrainRef:
    """Parsed OCI reference for a Boltzmann brain.

    Forms accepted::

        name
        name:tag
        namespace/name
        namespace/name:tag
        registry/namespace/name
        registry/namespace/name:tag
        registry/namespace/name@sha256:...

    A leading ``brains/`` is treated as the publishing namespace when no
    registry is present, e.g. ``brains/my-brain:latest`` becomes
    ``<registry_default>/brains/my-brain:latest``.
    """

    registry: str
    namespace: str
    name: str
    tag: str = "latest"
    digest: str | None = None

    @property
    def reference(self) -> str:
        """Full ``registry/namespace/name`` (no tag or digest)."""
        return f"{self.registry}/{self.namespace}/{self.name}"

    @property
    def target(self) -> str:
        """``reference:tag`` or ``reference@sha256:...``."""
        if self.digest:
            return f"{self.reference}@{self.digest}"
        return f"{self.reference}:{self.tag}"

    def with_tag(self, tag: str) -> BrainRef:
        """Return a copy with the given tag and no digest."""
        return BrainRef(
            registry=self.registry,
            namespace=self.namespace,
            name=self.name,
            tag=tag,
            digest=None,
        )


def parse_brain_ref(
    ref: str,
    *,
    registry_default: str = "registry.alquimia.ai",
    namespace_default: str = "brains",
) -> BrainRef:
    """Parse *ref* into a full brain reference, expanding short forms.

    Examples::

        >>> parse_brain_ref("brains/my-brain:latest")
        BrainRef(registry='registry.alquimia.ai', namespace='brains',
                 name='my-brain', tag='latest', digest=None)

        >>> parse_brain_ref("registry.example.com/acme/handbook:main",
        ...                 registry_default="registry.alquimia.ai")
        BrainRef(registry='registry.example.com', namespace='acme',
                 name='handbook', tag='main', digest=None)
    """
    if not ref or not isinstance(ref, str):
        raise ValueError(f"brain reference must be a non-empty string, got {ref!r}")

    original = ref
    digest: str | None = None
    tag: str = "latest"

    if "@" in ref:
        ref, digest = ref.rsplit("@", 1)
        if not digest.startswith("sha256:") or len(digest) != 71:
            raise ValueError(f"invalid digest in {original!r}")
    else:
        last_slash = ref.rfind("/")
        colon = ref.rfind(":")
        if colon > last_slash:
            tag = ref[colon + 1 :]
            ref = ref[:colon]

    parts = [p for p in ref.split("/") if p]
    if not parts:
        raise ValueError(f"empty brain reference: {original!r}")

    if len(parts) == 1:
        # "name" or "name:tag" -> use default registry and namespace
        registry = registry_default
        namespace = namespace_default
        name = parts[0]
    elif len(parts) == 2:
        first = parts[0]
        if _looks_like_registry(first):
            registry = first
            namespace = namespace_default
            name = parts[1]
        else:
            # "namespace/name" or "brains/name"
            registry = registry_default
            namespace = first
            name = parts[1]
    elif len(parts) == 3:
        registry = parts[0]
        namespace = parts[1]
        name = parts[2]
    else:
        # "registry/deep/namespace/name"
        registry = parts[0]
        name = parts[-1]
        namespace = "/".join(parts[1:-1])

    if not name:
        raise ValueError(f"empty brain name in {original!r}")

    return BrainRef(
        registry=registry,
        namespace=namespace,
        name=name,
        tag=tag,
        digest=digest,
    )


def expand_brain_ref(
    ref: str,
    *,
    registry_default: str = "registry.alquimia.ai",
    namespace_default: str = "brains",
) -> str:
    """Expand a short brain reference to a full ``registry/namespace/name:tag``.

    This is a convenience wrapper around :func:`parse_brain_ref` that returns
    the string form suitable for passing to the registry client or cache.
    """
    return parse_brain_ref(
        ref,
        registry_default=registry_default,
        namespace_default=namespace_default,
    ).target


def expand_namespace(
    namespace: str,
    *,
    registry_default: str = "registry.alquimia.ai",
) -> str:
    """Expand a short namespace like ``brains`` to ``registry.alquimia.ai/brains``.

    A namespace that already starts with a registry host is returned unchanged.
    """
    namespace = namespace.strip().rstrip("/")
    if not namespace:
        raise ValueError("namespace must not be empty")
    parts = namespace.split("/")
    if _looks_like_registry(parts[0]):
        return namespace
    return f"{registry_default.rstrip('/')}/{namespace}"
