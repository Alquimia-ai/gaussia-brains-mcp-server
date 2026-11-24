"""Write the REST contract (OpenAPI 3.1) to docs/openapi.json.

The spec is generated from the application, so it cannot drift from the code;
this script exists so the contract is reviewable in a diff and shareable with
clients (Ledger) without running the service. CI can check it is current with
``--check``.

    .venv/bin/python scripts/export_openapi.py          # write
    .venv/bin/python scripts/export_openapi.py --check  # fail if stale
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

TARGET = Path(__file__).resolve().parent.parent / "docs" / "openapi.json"


def build() -> str:
    for name in list(os.environ):
        if name.startswith("GBMS_"):
            del os.environ[name]
    os.environ["GBMS_AUTH_RESOURCE"] = "https://brains.example.com"

    from gaussia_brains_mcp_server.lifecycle.errors import STATUS
    from gaussia_brains_mcp_server.main import create_app

    spec = create_app().openapi()
    spec["info"]["description"] = (
        "Boltzmann brains over REST: discovery, and the full lifecycle -- create, "
        "evidence, commits, branches, push/pull, converge, import -- over the "
        "service's working store. Lifecycle errors carry a stable `code`: "
        + ", ".join(f"`{code}` ({status})" for code, status in STATUS.items())
        + ". Long operations answer 202 with a job (`GET /v1/jobs/{id}`)."
    )
    return json.dumps(spec, indent=2, sort_keys=True) + "\n"


def main() -> int:
    rendered = build()
    if "--check" in sys.argv:
        current = TARGET.read_text() if TARGET.exists() else ""
        if current != rendered:
            print(f"{TARGET} is stale; run scripts/export_openapi.py", file=sys.stderr)
            return 1
        return 0
    TARGET.write_text(rendered)
    print(f"wrote {TARGET}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
