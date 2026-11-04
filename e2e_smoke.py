"""End-to-end smoke test against a real registry brain."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time

import httpx
from loguru import logger
from mcp import Client

PORT = 8123
BASE = f"http://127.0.0.1:{PORT}"
BRAIN = "registry.alquimia.ai/brains/bpd-brain-2:latest"
MCP_URL = f"{BASE}/mcp/{BRAIN}"


def _env() -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "GBMS_AUTH_ENABLED": "false",
            "GBMS_ALLOW_INSECURE_DEV": "true",
            "GBMS_CATALOG_NAMESPACES": '["registry.alquimia.ai/brains"]',
            "GBMS_ALLOWED_SOURCES": '["registry.alquimia.ai/brains"]',
            "GBMS_VERIFY_UNSIGNED": "permit",
            "GBMS_MCP_TRANSPORT": "streamable-http",
            "GBMS_MCP_PATH": "/mcp",
            "GBMS_PORT": str(PORT),
            "GBMS_ORAS_BIN": "/opt/homebrew/bin/oras",
            "GBMS_LOG_LEVEL": "INFO",
        }
    )
    return env


async def _wait_for_server(timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    async with httpx.AsyncClient() as client:
        while time.time() < deadline:
            try:
                r = await client.get(f"{BASE}/v1/brains", timeout=2)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            await asyncio.sleep(0.5)
    raise RuntimeError("server did not start")


async def main() -> int:
    proc = subprocess.Popen(
        [sys.executable, "-m", "gaussia_brains_mcp_server.main"],
        cwd=os.path.dirname(__file__),
        env=_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        logger.info("Waiting for server...")
        await _wait_for_server()
        logger.info("Server ready")

        async with Client(
            server=MCP_URL,
            raise_exceptions=True,
        ) as client:
            info = client.server_info
            assert info is not None
            logger.info("Connected: {} {}", info.name, info.version)
            instructions = client.instructions or ""
            logger.info("Instructions: {}", instructions[:200])

            tools = await client.list_tools()
            tool_names = [t.name for t in tools.tools]
            logger.info("Tools: {}", tool_names)
            assert "brain_search" in tool_names
            assert "brain_read_block" in tool_names
            assert "brain_resolve" in tool_names

            logger.info("Calling brain_search...")
            result = await client.call_tool(
                "brain_search", {"query": "BPD", "limit": 3}
            )
            logger.info(
                "Search result: {}",
                result.model_dump() if hasattr(result, "model_dump") else result,
            )

            prompts = await client.list_prompts()
            prompt_names = [p.name for p in prompts.prompts]
            logger.info("Prompts: {}", prompt_names)
            assert "answer_from_brain" in prompt_names
            assert "explore_brain" in prompt_names

            logger.info("Calling brain_resolve...")
            resolve_result = await client.call_tool("brain_resolve", {})
            logger.info("Resolve result: {}", resolve_result)

        logger.info("E2E smoke test passed")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        if proc.stdout:
            print("--- server stdout ---")
            print(proc.stdout.read().decode("utf-8", errors="replace"))


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
