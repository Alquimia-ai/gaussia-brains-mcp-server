"""Embeddings over an OpenAI-compatible ``/v1/embeddings`` endpoint.

Replaces core's ``alquimia.core._get_embeddings`` (a LangChain connector built
from Alquimia's YAML config). Anything speaking the OpenAI embeddings API
works: vLLM, TEI, Ollama, LiteLLM, OpenAI itself.

The index engines call this synchronously -- ``Index.search`` and
``Index.build`` are synchronous in the SDK -- so the client is a blocking one.
"""

from __future__ import annotations

import threading
from typing import Protocol

import httpx
from loguru import logger

from gaussia_brains_mcp_server.config import Settings


class Embeddings(Protocol):
    """What the vector index needs: documents in, one vector per document out."""

    model_tag: str

    def embed_documents(self, texts: list[str]) -> list[list[float]]: ...


class EmbeddingsError(RuntimeError):
    """The embeddings endpoint did not return one vector per input."""


class OpenAICompatibleEmbeddings:
    """A blocking client for ``POST {url}/embeddings``."""

    def __init__(
        self,
        url: str,
        model: str,
        *,
        model_tag: str,
        api_key: str | None = None,
        timeout: float = 30.0,
        batch_size: int = 64,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.url = url.rstrip("/") + "/embeddings"
        self.model = model
        self.model_tag = model_tag
        self._batch = batch_size
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._http = httpx.Client(timeout=timeout, headers=headers, transport=transport)

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for start in range(0, len(texts), self._batch):
            batch = texts[start : start + self._batch]
            # An empty string is rejected by several servers; one space embeds as noise.
            payload = {"model": self.model, "input": [t or " " for t in batch]}
            response = self._http.post(self.url, json=payload)
            response.raise_for_status()
            data = response.json().get("data") or []
            ordered = sorted(data, key=lambda item: item.get("index", 0))
            if len(ordered) != len(batch):
                raise EmbeddingsError(
                    f"{self.url} returned {len(ordered)} vectors for {len(batch)} inputs"
                )
            vectors.extend([float(x) for x in item["embedding"]] for item in ordered)
        return vectors


_lock = threading.Lock()
_cached: tuple[tuple[str | None, ...], Embeddings | None] | None = None


def get_embeddings(settings: Settings) -> Embeddings | None:
    """The configured embeddings client, or ``None`` when none is configured."""
    global _cached
    key = (
        settings.embeddings_url,
        settings.embeddings_model,
        settings.effective_embeddings_model_tag(),
    )
    with _lock:
        if _cached is not None and _cached[0] == key:
            return _cached[1]
        client: Embeddings | None = None
        tag = settings.effective_embeddings_model_tag()
        if settings.embeddings_url and settings.embeddings_model and tag:
            api_key = (
                settings.embeddings_api_key.get_secret_value()
                if settings.embeddings_api_key
                else None
            )
            client = OpenAICompatibleEmbeddings(
                settings.embeddings_url,
                settings.embeddings_model,
                model_tag=tag,
                api_key=api_key,
                timeout=settings.embeddings_timeout_seconds,
                batch_size=settings.embeddings_batch_size,
            )
            logger.info("Embeddings: {} via {}", tag, settings.embeddings_url)
        _cached = (key, client)
        return client


def set_embeddings(client: Embeddings | None, settings: Settings) -> None:
    """Install *client* for *settings* (tests, or a host with its own client)."""
    global _cached
    with _lock:
        _cached = (
            (
                settings.embeddings_url,
                settings.embeddings_model,
                settings.effective_embeddings_model_tag(),
            ),
            client,
        )
