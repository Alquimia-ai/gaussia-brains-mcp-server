"""Per-brain MCP server.

Each brain gets its own :class:`mcp.server.Server` instance with generated
instructions, tools and prompts. The server is then wrapped by the MCP SDK's
StreamableHTTP session manager and mounted under ``/mcp/<reference>``.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, cast

import anyio
from anyio.abc import TaskStatus
from boltzmann import BlockId, MemoryType, OciDigest
from boltzmann.distribution.manifest import BrainManifest
from boltzmann.store import MemoryBlockStore
from loguru import logger
from mcp import types
from mcp.server import Server
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from mcp.server.auth.provider import AccessToken
from mcp.server.context import ServerRequestContext
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.types import Receive, Scope, Send

from gaussia_brains_mcp_server import __version__
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.auth.scopes import ALL as ALL_SCOPES
from gaussia_brains_mcp_server.auth.scopes import READ
from gaussia_brains_mcp_server.cache import BrainCache
from gaussia_brains_mcp_server.client import registry_client
from gaussia_brains_mcp_server.config import Settings, SignaturePolicy
from gaussia_brains_mcp_server.errors import describe_brain_error
from gaussia_brains_mcp_server.guidance import (
    BrainContext,
    build_answer_prompt,
    build_explore_prompt,
    build_instructions,
    build_prompts,
    build_tools,
    content_to_text,
)
from gaussia_brains_mcp_server.signatures import SignatureReport, get_signature_verifier


async def _fetch_metadata(
    client_factory: Any, reference: str, manifest: BrainManifest
) -> dict[str, Any]:
    """Pull the brain config blob, if present, and return title/description."""
    config = manifest.config
    if not config or not config.digest:
        return {}
    client = client_factory()
    store = MemoryBlockStore()
    try:
        digest = OciDigest.parse(config.digest)
        await client.pull_blob(reference, digest, store)
        raw = store.get_bytes(digest)
        data = json.loads(raw)
        if isinstance(data, dict):
            return {
                k: data[k]
                for k in ("title", "description")
                if k in data and isinstance(data[k], str)
            }
    except Exception as exc:  # pragma: no cover
        logger.debug("Could not load metadata for {}: {}", reference, exc)
    return {}


async def _signature_report(
    settings: Settings, target: str
) -> SignatureReport | None:
    if settings.signature_policy is SignaturePolicy.OFF:
        return None
    verifier = get_signature_verifier(settings)
    try:
        return verifier.inspect(target)
    except Exception as exc:  # pragma: no cover
        logger.debug("Signature inspection failed for {}: {}", target, exc)
        return None


@contextlib.asynccontextmanager
async def _brain_lifespan(
    reference: str,
    tag: str,
    settings: Settings,
    cache: BrainCache,
) -> AsyncIterator[BrainContext]:
    """Open the brain for this server instance and yield its context."""
    client = registry_client(pinned=True)
    manifest = await client.resolve(reference, tag)
    digest = client.last_digest or "unknown"
    metadata = await _fetch_metadata(registry_client, reference, manifest)
    brain = await cache.get_or_open(reference, tag=tag)
    report = await _signature_report(settings, f"{reference}@{digest}")
    ctx = BrainContext(
        reference=reference,
        tag=tag,
        digest=digest,
        manifest=manifest,
        brain=brain,
        signature_report=report,
        metadata=metadata,
    )
    logger.info("MCP server ready for {}@{} ({})", reference, tag, digest[:19])
    yield ctx


def _context(
    request_context: ServerRequestContext[BrainContext],
) -> BrainContext:
    return request_context.lifespan_context


def _tool_error(message: str, code: str = "INTERNAL") -> types.CallToolResult:
    return types.CallToolResult(
        is_error=True,
        content=[types.TextContent(type="text", text=json.dumps({"code": code, "message": message}))],
    )


def _ok_text(data: Any) -> types.CallToolResult:
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=json.dumps(data, ensure_ascii=False, default=str))]
    )


def _parse_memory_types(values: list[str]) -> list[MemoryType]:
    return [MemoryType(v) for v in values]


async def _handle_search(
    ctx: BrainContext, arguments: dict[str, Any]
) -> types.CallToolResult:
    query_text = arguments.get("query", "")
    if not isinstance(query_text, str):
        return _tool_error("query must be a string", "QUERY_INVALID")
    limit = int(arguments.get("limit", 10))
    expand_depth = int(arguments.get("expand_depth", 0))
    raw_types = arguments.get("memory_types")
    filters: dict[str, Any] = {}
    if raw_types is not None:
        if not isinstance(raw_types, list) or not all(isinstance(t, str) for t in raw_types):
            return _tool_error("memory_types must be a list of strings", "MEMORY_TYPE_INVALID")
        try:
            filters["memory_types"] = _parse_memory_types(raw_types)
        except ValueError as exc:
            return _tool_error(f"invalid memory type: {exc}", "MEMORY_TYPE_INVALID")

    from boltzmann.query import Query, QueryFilters, QueryHints

    query = Query(
        text=query_text,
        filters=cast(QueryFilters, filters),
        hints=QueryHints(limit=limit, expand_depth=expand_depth),
    )
    try:
        bundle = ctx.brain.search(query)
    except Exception as exc:
        info = describe_brain_error(exc)
        return _tool_error(info.message, info.code)

    results = []
    for match in bundle.matches:
        if not match.verified:
            continue
        results.append(
            {
                "block_id": str(match.block_id),
                "memory_type": match.memory_type.value,
                "content": content_to_text(match.content),
                "score": match.score,
                "sources": [
                    {"block_id": str(src.block_id), "locator": src.locator}
                    for src in (match.sources or [])
                ],
                "locator": next((src.locator for src in (match.sources or [])), None),
                "resolvable": match.resolvable,
            }
        )
    return _ok_text(
        {
            "results": results,
            "truncated": bundle.truncated,
            "verified_against": {
                mt.value: str(root) for mt, root in bundle.verified_against.items()
            },
        }
    )


async def _handle_read_block(
    ctx: BrainContext, arguments: dict[str, Any]
) -> types.CallToolResult:
    raw_id = arguments.get("block_id")
    if not isinstance(raw_id, str):
        return _tool_error("block_id must be a string", "QUERY_INVALID")
    try:
        block_id = BlockId.parse(raw_id)
    except Exception as exc:
        return _tool_error(f"invalid block id: {exc}", "QUERY_INVALID")
    try:
        block = ctx.brain.resolve(block_id)
    except Exception as exc:
        info = describe_brain_error(exc)
        return _tool_error(info.message, info.code)

    payload = block.payload()
    return _ok_text(
        {
            "block_id": str(block.block_id),
            "memory_type": block.MEMORY_TYPE.value,
            "content": content_to_text(payload),
            "raw": payload,
        }
    )


async def _handle_resolve(ctx: BrainContext) -> types.CallToolResult:
    report = ctx.signature_report
    authorship = ctx.brain._authorship()
    return _ok_text(
        {
            "reference": ctx.reference,
            "tag": ctx.tag,
            "digest": ctx.digest,
            "modules": [mt.value for mt in ctx.manifest.modules],
            "signature": report.summary() if report else {"checked": False},
            "authorship": authorship.model_dump(mode="json") if authorship else None,
        }
    )


async def _on_call_tool(
    request_context: ServerRequestContext[BrainContext],
    params: types.CallToolRequestParams,
) -> types.CallToolResult:
    ctx = _context(request_context)
    try:
        if params.name == "brain_search":
            return await _handle_search(ctx, params.arguments or {})
        if params.name == "brain_read_block":
            return await _handle_read_block(ctx, params.arguments or {})
        if params.name == "brain_resolve":
            return await _handle_resolve(ctx)
        return _tool_error(f"unknown tool: {params.name}", "INTERNAL")
    except Exception as exc:  # pragma: no cover
        logger.exception("tool {} failed", params.name)
        info = describe_brain_error(exc)
        return _tool_error(info.message, info.code)


async def _on_list_tools(
    request_context: ServerRequestContext[BrainContext],
    _params: types.PaginatedRequestParams | None,
) -> types.ListToolsResult:
    ctx = _context(request_context)
    tools = [
        types.Tool.model_validate(t) for t in build_tools(ctx)
    ]
    return types.ListToolsResult(tools=tools)


async def _on_list_prompts(
    _request_context: ServerRequestContext[BrainContext],
    _params: types.PaginatedRequestParams | None,
) -> types.ListPromptsResult:
    prompts = [types.Prompt.model_validate(p) for p in build_prompts()]
    return types.ListPromptsResult(prompts=prompts)


async def _on_get_prompt(
    _request_context: ServerRequestContext[BrainContext],
    params: types.GetPromptRequestParams,
) -> types.GetPromptResult:
    name = params.name
    arguments = params.arguments or {}
    if name == "answer_from_brain":
        text = build_answer_prompt(arguments.get("question", ""))
    elif name == "explore_brain":
        text = build_explore_prompt(arguments.get("topic", ""))
    else:
        return types.GetPromptResult(
            description="Unknown prompt",
            messages=[],
        )
    return types.GetPromptResult(
        description=text.splitlines()[0],
        messages=[
            types.PromptMessage(
                role="user",
                content=types.TextContent(type="text", text=text),
            )
        ],
    )


def create_brain_server(
    reference: str,
    tag: str,
    settings: Settings,
    cache: BrainCache,
) -> Server[BrainContext]:
    """Create an MCP server bound to one brain reference and tag."""

    @contextlib.asynccontextmanager
    async def lifespan(server: Server[BrainContext]) -> AsyncIterator[BrainContext]:
        async with _brain_lifespan(reference, tag, settings, cache) as ctx:
            server.instructions = build_instructions(ctx)
            yield ctx

    return Server(
        name="gaussia-brain",
        version=__version__,
        instructions="",  # filled in lifespan after the brain is opened
        lifespan=lifespan,
        on_list_tools=_on_list_tools,
        on_call_tool=_on_call_tool,
        on_list_prompts=_on_list_prompts,
        on_get_prompt=_on_get_prompt,
    )


def create_brain_session_manager(
    reference: str,
    tag: str,
    settings: Settings,
    cache: BrainCache,
    *,
    json_response: bool = False,
    stateless: bool = False,
) -> StreamableHTTPSessionManager:
    """Create a StreamableHTTP session manager for one brain."""
    server = create_brain_server(reference, tag, settings, cache=cache)
    security = TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    )
    return StreamableHTTPSessionManager(
        app=server,
        json_response=json_response,
        stateless=stateless,
        security_settings=security,
    )


async def _run_manager(
    manager: StreamableHTTPSessionManager,
    *,
    task_status: TaskStatus[None] = anyio.TASK_STATUS_IGNORED,
) -> None:
    """Keep a session manager's lifespan open inside an anyio task group."""
    async with manager.run():
        task_status.started()
        await anyio.sleep_forever()


async def start_brain_manager(
    task_group: Any,
    manager: StreamableHTTPSessionManager,
) -> None:
    """Start *manager* inside *task_group* and wait for it to be ready."""
    await task_group.start(_run_manager, manager)


class _AuthCredentials:
    def __init__(self, scopes: list[str]) -> None:
        self.scopes = scopes


async def _send_json(
    send: Send,
    status_code: int,
    body: dict[str, Any],
    headers: list[tuple[bytes, bytes]] | None = None,
) -> None:
    payload = json.dumps(body).encode()
    all_headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(payload)).encode()),
    ]
    if headers:
        all_headers.extend(headers)
    await send({"type": "http.response.start", "status": status_code, "headers": all_headers})
    await send({"type": "http.response.body", "body": payload})


class BrainMcpAsgiApp:
    """ASGI app that authenticates and then dispatches to a brain session manager."""

    def __init__(
        self,
        reference: str,
        tag: str,
        settings: Settings,
        cache: BrainCache,
        verify_token: Callable[[str], Awaitable[Principal]],
        resource_metadata_url: str | None = None,
        *,
        json_response: bool = False,
        stateless: bool = False,
        dev_mode: bool = False,
    ) -> None:
        self.manager = create_brain_session_manager(
            reference,
            tag,
            settings,
            cache=cache,
            json_response=json_response,
            stateless=stateless,
        )
        self.verify_token = verify_token
        self.resource_metadata_url = resource_metadata_url
        self.dev_mode = dev_mode

    async def start(self, task_group: Any) -> None:
        await start_brain_manager(task_group, self.manager)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.manager.handle_request(scope, receive, send)
            return

        if self.dev_mode:
            self._inject_dev_user(scope)
            await self.manager.handle_request(scope, receive, send)
            return

        request = Request(scope, receive)
        auth_header = request.headers.get("authorization", "")
        if not auth_header.lower().startswith("bearer "):
            await self._send_challenge(send, "invalid_token", "Missing bearer token")
            return

        token = auth_header[7:]
        try:
            principal = await self.verify_token(token)
        except Exception as exc:
            info = describe_brain_error(exc)
            await self._send_challenge(send, "invalid_token", info.message)
            return

        if not principal.has_scope(READ):
            await self._send_scope_error(send)
            return

        # Inject an SDK-compatible authenticated user so session ownership works.
        access_token = AccessToken(
            token=token,
            client_id=principal.client_id or principal.subject,
            scopes=list(principal.scopes),
            subject=principal.subject,
            claims={"iss": principal.issuer, "x-gbms-principal": principal},
        )
        scope["user"] = AuthenticatedUser(access_token)
        scope["auth"] = _AuthCredentials(list(principal.scopes))
        await self.manager.handle_request(scope, receive, send)

    def _inject_dev_user(self, scope: Scope) -> None:
        access_token = AccessToken(
            token="dev",
            client_id="dev-anonymous",
            scopes=list(ALL_SCOPES),
            subject="dev-anonymous",
            claims={"iss": "urn:gaussia:insecure-dev"},
        )
        scope["user"] = AuthenticatedUser(access_token)
        scope["auth"] = _AuthCredentials(list(ALL_SCOPES))

    async def _send_challenge(
        self, send: Send, error: str, description: str, status_code: int = 401
    ) -> None:
        parts = [f'error="{error}"', f'error_description="{description.replace(chr(34), chr(39))}"']
        if self.resource_metadata_url:
            parts.append(f'resource_metadata="{self.resource_metadata_url}"')
        await _send_json(
            send,
            status_code,
            {"error": error, "error_description": description},
            headers=[(b"www-authenticate", ("Bearer " + ", ".join(parts)).encode())],
        )

    async def _send_scope_error(self, send: Send) -> None:
        parts = [
            'error="insufficient_scope"',
            f'scope="{READ}"',
        ]
        if self.resource_metadata_url:
            parts.append(f'resource_metadata="{self.resource_metadata_url}"')
        await _send_json(
            send,
            403,
            {"error": "insufficient_scope", "error_description": f"Required scope: {READ}"},
            headers=[(b"www-authenticate", ("Bearer " + ", ".join(parts)).encode())],
        )
