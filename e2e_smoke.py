"""End-to-end smoke test against a real registry brain."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import tempfile
import time

import httpx
from loguru import logger
from mcp import Client

PORT = 8123
BASE = f"http://127.0.0.1:{PORT}"
BRAIN_SHORT = "brains/bpd-brain-2:latest"
BRAIN_FULL = "registry.alquimia.ai/brains/bpd-brain-2:latest"
MCP_URL = f"{BASE}/mcp/{BRAIN_SHORT}"


def _env() -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "GBMS_AUTH_ENABLED": "false",
            "GBMS_ALLOW_INSECURE_DEV": "true",
            "GBMS_CATALOG_NAMESPACES": '["registry.alquimia.ai/brains"]',
            "GBMS_ALLOWED_SOURCES": '["registry.alquimia.ai/brains"]',
            "GBMS_VERIFY_UNSIGNED": "permit",
            "GBMS_SIGNATURE_POLICY": "off",
            "GBMS_MCP_TRANSPORT": "streamable-http",
            "GBMS_MCP_PATH": "/mcp",
            "GBMS_PORT": str(PORT),
            "GBMS_ORAS_BIN": "/opt/homebrew/bin/oras",
            "GBMS_LOG_LEVEL": "INFO",
        }
    )
    return env


async def _wait_for_server(timeout: float = 90.0) -> None:
    # The first catalog read can take several seconds while oras fetches the
    # namespace index from the registry, so give each request a generous timeout.
    await asyncio.sleep(2)
    deadline = time.time() + timeout
    async with httpx.AsyncClient() as client:
        while time.time() < deadline:
            try:
                r = await client.get(f"{BASE}/v1/brains", timeout=15)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            await asyncio.sleep(0.5)
    raise RuntimeError("server did not start")


async def _test_rest(client: httpx.AsyncClient) -> None:
    logger.info("Testing REST list...")
    r = await client.get(f"{BASE}/v1/brains", timeout=15)
    assert r.status_code == 200, f"list failed: {r.status_code} {r.text}"
    data = r.json()
    assert "entries" in data
    logger.info("REST list returned {} entries", len(data["entries"]))

    logger.info("Testing REST detail with short ref...")
    r = await client.get(f"{BASE}/v1/brains/{BRAIN_SHORT}", timeout=15)
    assert r.status_code == 200, f"detail failed: {r.status_code} {r.text}"
    detail = r.json()
    assert detail["reference"] == "registry.alquimia.ai/brains/bpd-brain-2"
    assert detail["signature"]["checked"] is False
    logger.info("REST detail digest: {}", detail["digest"])


async def _test_mcp() -> None:
    logger.info("Testing MCP connection with short ref {}...", BRAIN_SHORT)
    async with Client(server=MCP_URL, raise_exceptions=True) as client:
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
        # Signatures are disabled for this test run.
        assert "checked: false" in str(resolve_result).lower() or "false" in str(resolve_result)


async def main() -> int:
    log_file = tempfile.NamedTemporaryFile(
        mode="wb", suffix=".log", prefix="gbms_e2e_", delete=False
    )
    log_path = log_file.name
    log_file.close()
    proc = subprocess.Popen(
        [sys.executable, "-m", "gaussia_brains_mcp_server.main"],
        cwd=os.path.dirname(__file__),
        env=_env(),
        stdout=open(log_path, "wb"),
        stderr=subprocess.STDOUT,
    )
    try:
        logger.info("Waiting for server...")
        await _wait_for_server()
        logger.info("Server ready")

        async with httpx.AsyncClient() as client:
            await _test_rest(client)

        await _test_mcp()

        logger.info("E2E smoke test passed")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        print("--- server stdout ---")
        with open(log_path, "rb") as f:
            print(f.read().decode("utf-8", errors="replace"))
        os.unlink(log_path)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
