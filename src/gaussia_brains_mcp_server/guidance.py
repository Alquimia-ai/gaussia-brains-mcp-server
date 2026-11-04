"""Generated MCP guidance for one brain.

Instructions, tool descriptions and prompts are built from the brain's manifest,
its signature report and the modules it actually contains. They are regenerated
whenever a followed tag moves, so a bare MCP connection is enough to use a brain
well.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from boltzmann import Brain, MemoryType
from boltzmann.distribution.manifest import BrainManifest

from gaussia_brains_mcp_server.signatures import SignatureReport


@dataclass(frozen=True)
class BrainContext:
    """Everything needed to render guidance for one brain."""

    reference: str
    tag: str
    digest: str
    manifest: BrainManifest
    brain: Brain
    signature_report: SignatureReport | None = None
    metadata: dict[str, Any] | None = None


# Human-readable meaning of each module. Kept short; instructions expand on it.
_MODULE_MEANING: dict[MemoryType, str] = {
    MemoryType.CANONICAL: "source documents and passages",
    MemoryType.SEMANTIC: "facts, concepts and assertions",
    MemoryType.EPISODIC: "events and observations",
    MemoryType.PROCEDURAL: "how-to guides, procedures and recipes",
    MemoryType.PROVENANCE: "origins, authorship and lineage",
}


def _module_name(mt: MemoryType) -> str:
    return mt.value.capitalize()


def _title(ctx: BrainContext) -> str:
    md = ctx.metadata or {}
    return md.get("title") or ctx.manifest.annotations.get("org.opencontainers.image.title") or ctx.reference


def _description(ctx: BrainContext) -> str:
    md = ctx.metadata or {}
    return (
        md.get("description")
        or ctx.manifest.annotations.get("org.opencontainers.image.description")
        or "A Boltzmann brain served by Gaussia Brains MCP Server."
    )


def _signature_sentence(ctx: BrainContext) -> str:
    report = ctx.signature_report
    if report is None or not report.checked:
        return "No signature check was performed on this snapshot."
    if report.trusted:
        signers = ", ".join(s.describe() for s in report.signers) or "a trusted signer"
        return f"This server trusts the signature on this snapshot ({signers})."
    if report.signers:
        return (
            "This snapshot is signed by "
            + ", ".join(s.describe() for s in report.signers)
            + ", but this server does not trust any of those signers."
        )
    return "This snapshot has no usable publisher signature."


def build_instructions(ctx: BrainContext) -> str:
    """MCP server instructions for the brain currently being served."""
    modules = ctx.manifest.modules
    module_lines = "\n".join(
        f"- {_module_name(mt)} ({mt.value}): {_MODULE_MEANING[mt]}"
        for mt in modules
    )

    return f"""You are connected to the brain "{_title(ctx)}".

{_description(ctx)}

Modules available in this brain:
{module_lines}

{_signature_sentence(ctx)}
Snapshot digest: {ctx.digest}

How to work with this brain:
- Search with a few specific keywords in the language of the sources.
- Results are verified blocks, not prose: read them and draw your own conclusion.
- Always cite block ids when you use a block in an answer.
- Use `expand_depth` to follow relations when a result points to others.
- Use `brain_read_block` to open a cited source.
- An empty search result means the brain holds nothing on that query; do not treat it as an error.
"""


def _memory_type_enum(modules: list[MemoryType]) -> list[str]:
    return [mt.value for mt in modules]


def _search_tool_schema(modules: list[MemoryType]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": (
                    "Keywords or question to search for. Use the language and "
                    "terminology of the brain's sources."
                ),
            },
            "memory_types": {
                "type": "array",
                "items": {"type": "string", "enum": _memory_type_enum(modules)},
                "description": "Optional module filter. Omit to search all modules.",
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 100,
                "default": 10,
                "description": "Maximum number of results to return.",
            },
            "expand_depth": {
                "type": "integer",
                "minimum": 0,
                "maximum": 3,
                "default": 0,
                "description": "How many relation hops to expand from each result.",
            },
        },
        "required": ["query"],
    }


def _read_block_tool_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "block_id": {
                "type": "string",
                "description": "The block id to read, exactly as returned by brain_search.",
            },
        },
        "required": ["block_id"],
    }


def _resolve_tool_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {},
    }


def build_tools(ctx: BrainContext) -> list[dict[str, Any]]:
    """Tool definitions for ``on_list_tools``."""
    modules = ctx.manifest.modules
    ref = ctx.reference
    return [
        {
            "name": "brain_search",
            "description": (
                f"Search the brain {ref!r}. Returns verified blocks with "
                f"their block id, module type, content, score and source references. "
                f"Available modules: {', '.join(mt.value for mt in modules)}."
            ),
            "inputSchema": _search_tool_schema(modules),
        },
        {
            "name": "brain_read_block",
            "description": (
                "Read one verified block by id, including its full provenance. "
                "Use the block_id exactly as returned by brain_search."
            ),
            "inputSchema": _read_block_tool_schema(),
        },
        {
            "name": "brain_resolve",
            "description": (
                "Return metadata about the connected brain: digest, tag, modules, "
                "signature report and authorship state."
            ),
            "inputSchema": _resolve_tool_schema(),
        },
    ]


def _prompt_argument(name: str, description: str) -> dict[str, Any]:
    return {"name": name, "description": description, "required": True}


def build_prompts() -> list[dict[str, Any]]:
    """Prompt templates advertised by the server."""
    return [
        {
            "name": "answer_from_brain",
            "description": "Answer a question by searching the brain, reading cited blocks, and citing them.",
            "arguments": [_prompt_argument("question", "The question to answer from the brain.")],
        },
        {
            "name": "explore_brain",
            "description": "Survey what the brain holds on a topic.",
            "arguments": [_prompt_argument("topic", "The topic to explore.")],
        },
    ]


def build_answer_prompt(question: str) -> str:
    return (
        f"Answer the following question using only the connected brain.\n\n"
        f"Question: {question}\n\n"
        "First search the brain with relevant keywords. Then read the most relevant "
        "blocks with brain_read_block. Cite the block ids you use and explain how "
        "they support your answer. If the search returns nothing, say so."
    )


def build_explore_prompt(topic: str) -> str:
    return (
        f"Explore what the connected brain holds about: {topic}\n\n"
        "Search the brain, then summarize the main themes, facts and sources. "
        "Cite block ids for every claim. If the brain has little or nothing on the "
        "topic, state that clearly."
    )


def content_to_text(content: dict[str, Any]) -> str:
    """Best-effort text rendering of a block content dict.

    Canonical/semantic/procedural blocks usually carry a ``text`` or
    ``statement`` field; provenance blocks carry names/ids. Falls back to a
    compact JSON rendering.
    """
    for key in ("text", "statement", "passage", "content", "summary", "label"):
        value = content.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    # Some blocks hold a list of text fragments.
    for key in ("fragments", "lines", "items"):
        value = content.get(key)
        if isinstance(value, list):
            return "\n".join(str(v) for v in value)
    return json.dumps(content, ensure_ascii=False, default=str)
