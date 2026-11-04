"""MCP client side of the e2e smoke test (server must already be running)."""

from __future__ import annotations

import asyncio
import sys

from loguru import logger
from mcp import Client

MCP_URL = "http://127.0.0.1:8123/mcp/registry.alquimia.ai/brains/bpd-brain-2:latest"


async def main() -> int:
    async with Client(server=MCP_URL, raise_exceptions=True) as client:
        info = client.server_info
        assert info is not None
        logger.info("Connected: {} {}", info.name, info.version)
        instructions = client.instructions or ""
        logger.info("Instructions preview: {}", instructions[:200])

        tools = await client.list_tools()
        tool_names = [t.name for t in tools.tools]
        logger.info("Tools: {}", tool_names)
        assert "brain_search" in tool_names
        assert "brain_read_block" in tool_names
        assert "brain_resolve" in tool_names

        logger.info("Calling brain_search...")
        result = await client.call_tool("brain_search", {"query": "BPD", "limit": 3})
        logger.info("Search result items: {}", len(result.content))

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


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
