"""Embeddings over an OpenAI-compatible ``/v1/embeddings`` endpoint.

Replaces core's ``alquimia.core._get_embeddings`` (a LangChain connector built
from Alquimia's YAML config). Anything speaking the OpenAI embeddings API
works: vLLM, TEI, Ollama, LiteLLM, OpenAI itself.

The index engines call this synchronously -- ``Index.search`` and
``Index.build`` are synchronous in the SDK -- so the client is a blocking one.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from email.utils import parsedate_to_datetime
from typing import Protocol
from urllib.parse import urlsplit, urlunsplit

import httpx
from loguru import logger

from gaussia_brains_mcp_server.config import Settings


class Embeddings(Protocol):
    """What the vector index needs: documents in, one vector per document out."""

    model_tag: str

    def embed_documents(self, texts: list[str]) -> list[list[float]]: ...


class EmbeddingsError(RuntimeError):
    """The embeddings endpoint did not return one vector per input."""


def redact_url(url: str) -> str:
    """*url* without userinfo, query string or fragment, for logs and errors."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return "<unparseable url>"
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


def _retry_after_seconds(response: httpx.Response) -> float | None:
    """``Retry-After`` as seconds (delta-seconds or an HTTP date), if present."""
    value = response.headers.get("retry-after")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
    except (TypeError, ValueError, IndexError):
        return None


class OpenAICompatibleEmbeddings:
    """A blocking client for ``POST {url}/embeddings``.

    Transport errors, 429 and 5xx are retried up to *max_retries* times with
    exponential backoff and full jitter, honouring ``Retry-After`` up to
    *backoff_max*. Any other status fails at once.
    """

    def __init__(
        self,
        url: str,
        model: str,
        *,
        model_tag: str,
        api_key: str | None = None,
        timeout: float = 30.0,
        batch_size: int = 64,
        max_retries: int = 3,
        backoff: float = 0.5,
        backoff_max: float = 8.0,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.url = url.rstrip("/") + "/embeddings"
        self.model = model
        self.model_tag = model_tag
        self._batch = batch_size
        self._max_retries = max_retries
        self._backoff = backoff
        self._backoff_max = backoff_max
        self._sleep = sleep
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        # Every request carries a timeout: connect, read, write and pool.
        self._http = httpx.Client(timeout=timeout, headers=headers, transport=transport)

    @property
    def safe_url(self) -> str:
        return redact_url(self.url)

    def _delay(self, attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return min(retry_after, self._backoff_max)
        ceiling = min(self._backoff_max, self._backoff * (2**attempt))
        return random.uniform(0, ceiling)  # noqa: S311 - jitter, not crypto

    def _post(self, payload: dict) -> httpx.Response:
        attempt = 0
        while True:
            retry_after: float | None = None
            try:
                response = self._http.post(self.url, json=payload)
            except httpx.TransportError as exc:
                if attempt >= self._max_retries:
                    raise EmbeddingsError(
                        f"{self.safe_url} unreachable after {attempt + 1} attempt(s): "
                        f"{type(exc).__name__}"
                    ) from exc
                reason = type(exc).__name__
            else:
                status = response.status_code
                if status != 429 and status < 500:
                    response.raise_for_status()
                    return response
                if attempt >= self._max_retries:
                    raise EmbeddingsError(
                        f"{self.safe_url} answered {status} after {attempt + 1} attempt(s)"
                    )
                reason = str(status)
                retry_after = _retry_after_seconds(response)
            delay = self._delay(attempt, retry_after)
            attempt += 1
            logger.warning(
                "Embeddings {} failed ({}); retry {}/{} in {:.2f}s",
                self.safe_url,
                reason,
                attempt,
                self._max_retries,
                delay,
            )
            self._sleep(delay)

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for start in range(0, len(texts), self._batch):
            batch = texts[start : start + self._batch]
            # An empty string is rejected by several servers; one space embeds as noise.
            payload = {"model": self.model, "input": [t or " " for t in batch]}
            response = self._post(payload)
            data = response.json().get("data") or []
            ordered = sorted(data, key=lambda item: item.get("index", 0))
            if len(ordered) != len(batch):
                raise EmbeddingsError(
                    f"{self.safe_url} returned {len(ordered)} vectors for {len(batch)} inputs"
                )
            vectors.extend([float(x) for x in item["embedding"]] for item in ordered)
        return vectors


_lock = threading.Lock()
_cached: tuple[tuple[object, ...], Embeddings | None] | None = None


def _cache_key(settings: Settings) -> tuple[object, ...]:
    """Everything the client is built from: a change to any one rebuilds it."""
    api_key = (
        settings.embeddings_api_key.get_secret_value()
        if settings.embeddings_api_key
        else None
    )
    return (
        settings.embeddings_url,
        settings.embeddings_model,
        settings.effective_embeddings_model_tag(),
        api_key,
        settings.embeddings_timeout_seconds,
        settings.embeddings_batch_size,
        settings.embeddings_max_retries,
        settings.embeddings_backoff_seconds,
        settings.embeddings_backoff_max_seconds,
    )


def get_embeddings(settings: Settings) -> Embeddings | None:
    """The configured embeddings client, or ``None`` when none is configured."""
    global _cached
    key = _cache_key(settings)
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
                max_retries=settings.embeddings_max_retries,
                backoff=settings.embeddings_backoff_seconds,
                backoff_max=settings.embeddings_backoff_max_seconds,
            )
            logger.info(
                "Embeddings: {} via {}", tag, redact_url(settings.embeddings_url)
            )
        _cached = (key, client)
        return client


def set_embeddings(client: Embeddings | None, settings: Settings) -> None:
    """Install *client* for *settings* (tests, or a host with its own client)."""
    global _cached
    with _lock:
        _cached = (_cache_key(settings), client)
