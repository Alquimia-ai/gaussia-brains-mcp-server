"""The published REST contract (docs/openapi.json) matches the code."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_openapi_document_is_current(monkeypatch) -> None:
    spec = importlib.util.spec_from_file_location(
        "export_openapi", ROOT / "scripts" / "export_openapi.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    rendered = module.build()
    current = (ROOT / "docs" / "openapi.json").read_text()
    assert rendered == current, "run: .venv/bin/python scripts/export_openapi.py"
