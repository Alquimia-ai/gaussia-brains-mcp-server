"""Entry point: FastAPI + per-brain MCP dispatcher, and stdio mode."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any

import anyio
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from loguru import logger
from starlette.types import Receive, Scope, Send

from gaussia_brains_mcp_server import __version__
from gaussia_brains_mcp_server.api.brains import router as brains_router
from gaussia_brains_mcp_server.auth.http import metadata_router, resource_metadata_url
from gaussia_brains_mcp_server.auth.scopes import READ
from gaussia_brains_mcp_server.auth.verifier import TokenVerifier
from gaussia_brains_mcp_server.cache import BrainCache
from gaussia_brains_mcp_server.config import Settings, get_settings
from gaussia_brains_mcp_server.mcp_server import BrainMcpAsgiApp, create_brain_server


def _parse_brain_ref(brain_ref: str, settings: Settings) -> tuple[str, str]:
    """Expand short refs and split a brain reference into (reference, tag-or-digest)."""
    from gaussia_brains_mcp_server.cache import split_reference

    expanded = settings.expand_brain_ref(brain_ref)
    return split_reference(expanded)


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


class McpDispatchApp:
    """ASGI app mounted under ``/mcp`` that dispatches each brain to its own manager.

    Using a mounted ASGI app instead of a FastAPI route keeps the raw
    ``scope``/``receive``/``send`` channels intact, which the MCP SDK's
    StreamableHTTP manager needs.
    """

    def __init__(self, fastapi_app: FastAPI) -> None:
        self.fastapi_app = fastapi_app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await _send_json(send, 404, {"detail": "Not found"})
            return

        settings: Settings = self.fastapi_app.state.settings
        root_path = scope.get("root_path", "")
        path = scope["path"]
        prefix = root_path.rstrip("/") + "/"
        brain_ref = path[len(prefix):] if path.startswith(prefix) else path.lstrip("/")
        if not brain_ref:
            await _send_json(send, 404, {"detail": "Brain reference required"})
            return

        try:
            reference, tag = _parse_brain_ref(brain_ref, settings)
        except ValueError as exc:
            await _send_json(send, 400, {"detail": f"Invalid brain reference: {exc}"})
            return
        if not settings.source_allowed(reference):
            await _send_json(
                send, 403, {"detail": f"Brain source not allowed: {reference}"}
            )
            return

        key = f"{reference}:{tag}"
        apps: dict[str, BrainMcpAsgiApp] = self.fastapi_app.state.mcp_apps
        app_obj = apps.get(key)

        if app_obj is None:
            async with self.fastapi_app.state.mcp_apps_lock:
                app_obj = apps.get(key)
                if app_obj is None:
                    dev_mode = not settings.auth_enabled and settings.allow_insecure_dev
                    metadata_url = (
                        resource_metadata_url(settings.auth_resource)
                        if settings.auth_enabled
                        else None
                    )
                    app_obj = BrainMcpAsgiApp(
                        reference=reference,
                        tag=tag,
                        settings=settings,
                        cache=self.fastapi_app.state.brain_cache,
                        verify_token=self.fastapi_app.state.token_verifier.verify,
                        resource_metadata_url=metadata_url,
                        json_response=False,
                        stateless=False,
                        dev_mode=dev_mode,
                    )
                    await app_obj.start(self.fastapi_app.state.mcp_task_group)
                    apps[key] = app_obj

        await app_obj(scope, receive, send)


@asynccontextmanager
async def lifespan(app: FastAPI) -> Any:
    """Start shared resources and keep a task group open for MCP managers."""
    settings = get_settings()
    app.state.settings = settings
    app.state.token_verifier = TokenVerifier(settings)
    app.state.brain_cache = BrainCache(settings)
    app.state.mcp_apps = {}  # type: ignore[misc]
    app.state.mcp_apps_lock = asyncio.Lock()

    for problem in settings.startup_problems():
        logger.warning("Startup problem: {}", problem)

    async with anyio.create_task_group() as tg:
        app.state.mcp_task_group = tg
        yield


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the FastAPI application."""
    settings = settings or get_settings()
    mcp_path = settings.mcp_path
    if not mcp_path.startswith("/"):
        mcp_path = "/" + mcp_path

    app = FastAPI(
        title="Gaussia Brains MCP Server",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.settings = settings

    app.include_router(metadata_router(settings))
    app.include_router(brains_router)
    app.mount(mcp_path, McpDispatchApp(app))

    @app.exception_handler(HTTPException)
    async def _http_exception_handler(_request: Any, exc: HTTPException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=dict(exc.headers) if exc.headers else None,
        )

    return app


async def _run_stdio(settings: Settings) -> None:
    """Run a single-brain MCP server over stdio."""
    brain_ref = settings.stdio_brain
    if not brain_ref:
        raise SystemExit(
            "GBMS_STDIO_BRAIN must be set when GBMS_MCP_TRANSPORT=stdio"
        )

    try:
        reference, tag = _parse_brain_ref(brain_ref, settings)
    except ValueError as exc:
        raise SystemExit(f"Invalid brain reference: {exc}") from exc
    if not settings.source_allowed(reference):
        raise SystemExit(f"Brain source not allowed: {reference}")

    token: str | None = None
    if settings.stdio_bearer_token is not None:
        token = settings.stdio_bearer_token.get_secret_value()

    verifier = TokenVerifier(settings)
    if token:
        principal = await verifier.verify(token)
        if not principal.has_scope(READ):
            raise SystemExit("Token is missing the brains:read scope")
    elif settings.auth_enabled or not settings.allow_insecure_dev:
        raise SystemExit(
            "GBMS_STDIO_BEARER_TOKEN is required unless GBMS_ALLOW_INSECURE_DEV=true"
        )

    cache = BrainCache(settings)
    server = create_brain_server(reference, tag, settings, cache)
    init_options = server.create_initialization_options()

    logger.info("Starting stdio MCP server for {}@{}", reference, tag)
    from mcp.server import stdio

    async with stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            init_options,
            raise_exceptions=True,
        )


def main() -> None:
    """CLI entry point."""
    settings = get_settings()
    if settings.mcp_transport == "stdio":
        anyio.run(_run_stdio, settings)
    else:
        import uvicorn

        app = create_app(settings)
        uvicorn.run(
            app,
            host=settings.host,
            port=settings.port,
            log_level=settings.log_level.lower(),
        )


if __name__ == "__main__":
    main()
