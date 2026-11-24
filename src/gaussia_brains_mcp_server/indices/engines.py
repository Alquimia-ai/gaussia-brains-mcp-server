"""Pure-Python implementations of the six Boltzmann index kinds.

Ported from ``alquimia.boltzmann.indices.engines``. What changed is only where
the outside world comes in: embeddings from :mod:`gaussia_brains_mcp_server.embeddings`
(an OpenAI-compatible endpoint) instead of Alquimia's YAML-configured connector,
and the text ceilings from ``GBMS_*`` settings. Scoring, tokenization and tie
breaking are unchanged, so the same brain and query give the same top-k here
as in core.
"""

from __future__ import annotations

import inspect
import json
import math
from collections import defaultdict
from collections.abc import Iterable
from typing import Any, ClassVar

from boltzmann import BlockId, IndexKind, MemoryType
from boltzmann.blocks import Block
from boltzmann.exceptions import DistributionError
from boltzmann.indices import AbstractIndex, ContentReader, TravellingIndex
from loguru import logger

from gaussia_brains_mcp_server.embeddings import Embeddings
from gaussia_brains_mcp_server.text import fold, tokenize

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


class EngineEnvironment:
    """What the engines read from outside: two text ceilings and the embeddings.

    Two ceilings, because they bound different costs: what reaches a reader is
    capped by ``content_max_chars``, what is indexed by ``index_max_chars``.
    Process-wide and set once at startup by
    :func:`gaussia_brains_mcp_server.indices.configure_engines`.
    """

    content_max_chars: int = 8192
    index_max_chars: int = 1_000_000
    embeddings: Embeddings | None = None


ENV = EngineEnvironment()


def _get_embeddings() -> Embeddings | None:
    return ENV.embeddings


def _named_content_text(
    block: Block, content: ContentReader, max_chars: int | None = None
) -> list[str]:
    """Text from the bytes a block names but does not carry.

    Since pyboltzmann 0.4 any memory type may name its datum by digest, so
    ``content_digests`` is asked rather than the block's type. Unreadable or
    non-text content is skipped: a selective install may legally lack a blob,
    and there is nothing to tokenize in an image.

    A digest named twice is read once: a canonical block whose normalized view
    is its own blob names the same bytes twice, and reading both would double
    every term frequency in the document.
    """
    max_chars = max_chars or ENV.content_max_chars
    parts: list[str] = []
    for digest in dict.fromkeys(block.content_digests):
        try:
            parts.append(content.get_bytes(digest).decode("utf-8")[:max_chars])
        except UnicodeDecodeError:
            continue
        except Exception as exc:  # noqa: BLE001 - a missing blob must not fail the build
            logger.warning(f"index: content {digest} is unreadable: {exc}")
    return parts


def _block_text(
    block: Block,
    content: ContentReader | None = None,
    max_chars: int | None = None,
) -> str:
    """Searchable text for a block: what it carries, plus what it names.

    When the block names content, the fields that only reference it -- the
    digests it names and their media type -- are left out: they say how the
    content is stored, not what it is about. Any other field is indexed, even
    one that happens to look like a digest. The result is capped at
    *max_chars* for the block as a whole.
    """
    max_chars = max_chars or ENV.content_max_chars
    payload = block.payload()
    references = {str(d) for d in block.content_digests}
    if isinstance(payload, dict):
        parts = [
            v
            for k, v in payload.items()
            if isinstance(v, str)
            and v not in references
            and not (references and k == "media_type")
        ]
    else:
        parts = [str(payload)]

    if content is not None:
        parts.extend(_named_content_text(block, content, max_chars))

    return " ".join(p for p in parts if p)[:max_chars]


def _score_to_str(score: float) -> str:
    """Convert a retrieval score to a deterministic string representation.

    The SDK forbids floats in hashed structures and raises
    ``NonDeterministicValueError``.
    """
    return f"{score:.6f}"


# --------------------------------------------------------------------------- #
# 1. Hash map — exact identifier look-up
# --------------------------------------------------------------------------- #


class HashMapIndex(AbstractIndex):
    """Deterministic hash-map for exact BlockId look-ups.

    ``rebuildable = True`` — any client can rebuild this from the blocks.
    """

    KIND: ClassVar[IndexKind] = IndexKind.HASH_MAP
    REBUILDABLE: ClassVar[bool] = True

    def __init__(self) -> None:
        self._map: dict[BlockId, Block] = {}

    def build(self, blocks: Iterable[Block], content: ContentReader) -> None:
        self._map = {}
        for block in blocks:
            self._map[block.block_id] = block

    def search(self, query: Any, limit: int = 10) -> list[tuple[BlockId, float]]:
        if isinstance(query, BlockId):
            if query in self._map:
                return [(query, 1.0)]
            return []
        if isinstance(query, str):
            # Try to parse as a block-id hex string
            try:
                bid = BlockId.parse(query)
                if bid in self._map:
                    return [(bid, 1.0)]
            except Exception:
                pass
            # Exact match against any string field
            for bid, _block in self._map.items():
                if str(bid) == query or str(bid.hex) == query:
                    return [(bid, 1.0)]
        return []


# --------------------------------------------------------------------------- #
# 2. B⁺ tree — ordered / range predicates
# --------------------------------------------------------------------------- #


class _BTreeNode:
    """Simple B⁺-tree node (leaf or internal)."""

    def __init__(self, leaf: bool = True) -> None:
        self.leaf = leaf
        self.keys: list[str] = []
        self.values: list[list[BlockId]] = []  # only used when leaf
        self.children: list[_BTreeNode] = []  # only used when internal


class BTreeIndex(AbstractIndex):
    """Ordered B⁺-tree index over an orderable block field.

    A B⁺ tree earns its cost only over a domain where ``<`` means something.
    Block ids are SHA-256 digests, so ordering them answers no question the
    hash map does not answer better; this index is therefore keyed on a
    payload field per memory type -- ``occurred_at`` for episodic, the record
    timestamp for provenance, ``label`` for semantic and procedural, ``size``
    for canonical -- and ``key`` overrides that choice.

    Timestamps make this work without a type conversion: the protocol stores
    them as fixed-width RFC 3339 UTC strings, so lexicographic order over the
    raw payload value is already chronological order.

    Blocks whose key is absent are not indexed; they are still reachable
    through every other index. ``rebuildable = True``.
    """

    KIND: ClassVar[IndexKind] = IndexKind.BTREE
    REBUILDABLE: ClassVar[bool] = True
    _ORDER: ClassVar[int] = 8

    #: Ordering key per memory type, first present field wins. Dotted paths
    #: descend into nested payload objects.
    DEFAULT_KEYS: ClassVar[dict[MemoryType, tuple[str, ...]]] = {
        MemoryType.EPISODIC: ("occurred_at", "ended_at"),
        MemoryType.PROVENANCE: ("record.at",),
        MemoryType.SEMANTIC: ("label", "subject"),
        MemoryType.PROCEDURAL: ("label", "goal"),
        MemoryType.CANONICAL: ("size",),
    }

    #: Width used to left-pad integers so lexicographic order stays numeric.
    _INT_WIDTH: ClassVar[int] = 20

    def __init__(self, key: str | None = None) -> None:
        self._root = _BTreeNode(leaf=True)
        self._all: dict[str, list[BlockId]] = {}
        self._key = key

    def _key_fields(self, block: Block) -> tuple[str, ...]:
        if self._key is not None:
            return (self._key,)
        return self.DEFAULT_KEYS.get(block.MEMORY_TYPE, ())

    @staticmethod
    def _resolve(payload: dict[str, Any], path: str) -> Any:
        value: Any = payload
        for part in path.split("."):
            if not isinstance(value, dict):
                return None
            value = value.get(part)
            if value is None:
                return None
        return value

    def _normalize(self, value: Any) -> str | None:
        """Render a key value as a string whose order matches its own."""
        if isinstance(value, str):
            return value
        if isinstance(value, bool):
            return None
        if isinstance(value, int) and value >= 0:
            return str(value).zfill(self._INT_WIDTH)
        return None

    def _key_for(self, block: Block) -> str | None:
        payload = block.payload()
        for path in self._key_fields(block):
            normalized = self._normalize(self._resolve(payload, path))
            if normalized is not None:
                return normalized
        return None

    def build(self, blocks: Iterable[Block], content: ContentReader) -> None:
        self._root = _BTreeNode(leaf=True)
        self._all = {}
        skipped = 0
        for block in blocks:
            key = self._key_for(block)
            if key is None:
                skipped += 1
                continue
            self._all.setdefault(key, []).append(block.block_id)
        if skipped:
            logger.debug(f"BTreeIndex: {skipped} block(s) carry no ordering key")
        keys = sorted(self._all)
        if not keys:
            return
        self._root = self._build_tree(keys, 0)

    def _build_tree(self, keys: list[str], depth: int) -> _BTreeNode:
        order = self._ORDER
        if len(keys) <= order:
            node = _BTreeNode(leaf=True)
            node.keys = keys[:]
            node.values = [self._all[k] for k in keys]
            return node
        # Build internal node
        stride = max(1, len(keys) // order)
        split_points = list(range(stride, len(keys), stride))
        if not split_points or split_points[-1] == len(keys):
            split_points = split_points[:-1]
        node_keys = [keys[i] for i in split_points]
        children: list[_BTreeNode] = []
        start = 0
        for sp in split_points:
            children.append(self._build_tree(keys[start:sp], depth + 1))
            start = sp
        children.append(self._build_tree(keys[start:], depth + 1))
        node = _BTreeNode(leaf=False)
        node.keys = node_keys
        node.children = children
        return node

    def _collect(
        self,
        node: _BTreeNode,
        lo: str | None,
        hi: str | None,
        out: list[tuple[str, BlockId]],
        lo_exclusive: bool = False,
        hi_exclusive: bool = False,
    ) -> None:
        """Gather every entry in ``[lo, hi]``, skipping subtrees that cannot hold one.

        ``_build_tree`` puts every key strictly below ``node.keys[i]`` in
        ``children[i]``, and the rest in the trailing child, so each child spans
        ``[keys[i-1], keys[i])``. That bound is what lets a subtree be discarded
        without opening it -- which is the only reason to hold a tree here
        rather than a sorted list.
        """
        if node.leaf:
            for k, vals in zip(node.keys, node.values, strict=True):
                if lo is not None and (k < lo or (lo_exclusive and k == lo)):
                    continue
                if hi is not None and (k > hi or (hi_exclusive and k == hi)):
                    continue
                out.extend((k, bid) for bid in vals)
            return

        for i, child in enumerate(node.children):
            lower = node.keys[i - 1] if i > 0 else None
            upper = node.keys[i] if i < len(node.keys) else None
            if hi is not None and lower is not None and lower > hi:
                continue
            if lo is not None and upper is not None and upper <= lo:
                continue
            self._collect(child, lo, hi, out, lo_exclusive, hi_exclusive)

    def _range(
        self,
        lo: str | None,
        hi: str | None,
        limit: int,
        lo_exclusive: bool = False,
        hi_exclusive: bool = False,
    ) -> list[tuple[BlockId, float]]:
        """Highest keys first, so truncating to ``limit`` keeps the most recent."""
        found: list[tuple[str, BlockId]] = []
        self._collect(self._root, lo, hi, found, lo_exclusive, hi_exclusive)
        found.sort(key=lambda item: item[0], reverse=True)
        return [(bid, 1.0) for _key, bid in found[:limit]]

    def search(self, query: Any, limit: int = 10) -> list[tuple[BlockId, float]]:
        if isinstance(query, str):
            if query.endswith("*"):
                prefix = query[:-1]
                return self._range(prefix, prefix + "￿", limit)
            return [(bid, 1.0) for bid in self._all.get(query, [])[:limit]]
        if isinstance(query, dict):
            lo, lo_exclusive = query.get("gte"), False
            if lo is None:
                lo, lo_exclusive = query.get("gt"), True
            hi, hi_exclusive = query.get("lte"), False
            if hi is None:
                hi, hi_exclusive = query.get("lt"), True
            if lo is None and hi is None:
                return []
            return self._range(
                self._normalize(lo) if lo is not None else None,
                self._normalize(hi) if hi is not None else None,
                limit,
                lo_exclusive and lo is not None,
                hi_exclusive and hi is not None,
            )
        return []


# --------------------------------------------------------------------------- #
# 3. Inverted — term match
# --------------------------------------------------------------------------- #


class InvertedIndex(AbstractIndex):
    """BM25 inverted index over block text, accent-aware.

    Every word is posted twice: as written and folded (see
    ``gaussia_brains_mcp_server.text.fold``). A query term scores on both, the exact form
    at full weight and the folded form at ``FOLDED_WEIGHT``, so a query without
    diacritics still reaches text written with them, while an exact spelling
    ranks above a folded match.

    ``rebuildable = True``.
    """

    KIND: ClassVar[IndexKind] = IndexKind.INVERTED
    REBUILDABLE: ClassVar[bool] = True

    K1: ClassVar[float] = 1.2
    B: ClassVar[float] = 0.75
    FOLDED_WEIGHT: ClassVar[float] = 0.5

    def __init__(self) -> None:
        self._index: dict[str, set[BlockId]] = {}
        self._term_freq: dict[BlockId, dict[str, int]] = {}
        self._folded_index: dict[str, set[BlockId]] = {}
        self._folded_freq: dict[BlockId, dict[str, int]] = {}
        self._doc_lengths: dict[BlockId, int] = {}
        self._avg_len: float = 0.0

    def build(self, blocks: Iterable[Block], content: ContentReader) -> None:
        index: dict[str, set[BlockId]] = defaultdict(set)
        folded_index: dict[str, set[BlockId]] = defaultdict(set)
        self._term_freq = {}
        self._folded_freq = {}
        self._doc_lengths = {}
        total_len = 0
        for block in blocks:
            tokens = tokenize(_block_text(block, content, ENV.index_max_chars))
            tf: dict[str, int] = defaultdict(int)
            folded_tf: dict[str, int] = defaultdict(int)
            for t in tokens:
                tf[t] += 1
                folded_tf[fold(t)] += 1
            for t in tf:
                index[t].add(block.block_id)
            for t in folded_tf:
                folded_index[t].add(block.block_id)
            self._term_freq[block.block_id] = dict(tf)
            self._folded_freq[block.block_id] = dict(folded_tf)
            self._doc_lengths[block.block_id] = len(tokens)
            total_len += len(tokens)
        count = len(self._doc_lengths)
        self._avg_len = total_len / count if count else 0.0
        self._index = dict(index)
        self._folded_index = dict(folded_index)

    def _idf(self, postings: dict[str, set[BlockId]], term: str) -> float:
        """Okapi IDF: a term in every block weighs next to nothing.

        Common words are weighted down by how often they occur in the
        composition, in any language, without a word list. Deterministic: it
        depends on the composition alone.
        """
        n_docs = len(self._doc_lengths)
        df = len(postings.get(term, ()))
        return math.log((n_docs - df + 0.5) / (df + 0.5) + 1.0)

    def _bm25(self, tf: int, dl: int) -> float:
        norm = 1 - self.B + self.B * dl / max(self._avg_len, 1.0)
        return (tf * (self.K1 + 1)) / (tf + self.K1 * norm)

    def search(self, query: Any, limit: int = 10) -> list[tuple[BlockId, float]]:
        if not isinstance(query, str):
            return []
        # A query is a set of terms: repeating a word does not weigh it more.
        terms = list(dict.fromkeys(tokenize(query)))
        if not terms:
            return []
        weighted = [
            (
                t,
                self._idf(self._index, t),
                fold(t),
                self._idf(self._folded_index, fold(t)),
            )
            for t in terms
        ]
        candidates: set[BlockId] = set()
        for t, _idf, folded, _fidf in weighted:
            candidates |= self._index.get(t, set())
            candidates |= self._folded_index.get(folded, set())
        if not candidates:
            return []

        scores: dict[BlockId, float] = {}
        for bid in candidates:
            dl = self._doc_lengths.get(bid, 0)
            tf_map = self._term_freq.get(bid, {})
            folded_map = self._folded_freq.get(bid, {})
            score = 0.0
            for t, idf, folded, folded_idf in weighted:
                if tf := tf_map.get(t, 0):
                    score += idf * self._bm25(tf, dl)
                # Additive on purpose, even for a word with no diacritics
                # (whose two forms coincide): the exact channel is what ranks
                # an exact spelling above a folded-only match.
                if ftf := folded_map.get(folded, 0):
                    score += self.FOLDED_WEIGHT * folded_idf * self._bm25(ftf, dl)
            scores[bid] = score
        # Ties break on the block id, so equal scores order the same in every
        # process (set iteration order is not stable across processes).
        ranked = sorted(scores.items(), key=lambda x: (-x[1], str(x[0])))
        return [(bid, float(_score_to_str(s))) for bid, s in ranked[:limit]]


# --------------------------------------------------------------------------- #
# 4. Vector — semantic / natural-language intent
# --------------------------------------------------------------------------- #


class VectorIndex(AbstractIndex, TravellingIndex):
    """Dense-vector semantic index using the configured embeddings model.

    This is the sole *travelling* index: rebuilding it requires an embedding
    model that a model-agnostic client does not carry, so it ships inside the
    module's OCI layer via ``dump()`` / ``load()``.

    ``rebuildable = False``.
    """

    KIND: ClassVar[IndexKind] = IndexKind.VECTOR
    REBUILDABLE: ClassVar[bool] = False

    _DUMP_VERSION: ClassVar[int] = 1

    def __init__(self) -> None:
        self._vectors: dict[BlockId, list[float]] = {}
        self._model_tag: str | None = None
        self._mismatch_reported = False

    @property
    def model_tag(self) -> str | None:
        return self._model_tag

    @staticmethod
    def is_available() -> bool:
        """Whether this index can be built here.

        Asked through the same reference ``build`` will use, so the decision
        to register a ``VectorIndex`` and the decision to build one cannot
        disagree. When they did, registering succeeded and opening the brain
        then failed outright on a ``build`` that refuses to produce an empty
        index.
        """
        try:
            return _get_embeddings() is not None
        except Exception as exc:  # noqa: BLE001 - any failure means unavailable
            logger.warning(f"VectorIndex unavailable: {exc}")
            return False

    @staticmethod
    def _current_model_tag() -> str:
        """``provider/model`` of the embeddings this deployment is configured with."""
        embeddings = _get_embeddings()
        return embeddings.model_tag if embeddings is not None else "unknown"

    @classmethod
    def accepts(cls, published_model: str | None) -> bool:
        """Whether a travelling layer embedded by *published_model* is usable here.

        Asked before a pull downloads the layer. One embedded by another model
        could never be scored here, and its payload need not even be in this
        index's dump format, so loading it would fail the whole pull over an
        index nothing can query. An unannotated layer is let through, as
        ``load`` would.
        """
        if not cls.is_available():
            return False
        if published_model is None:
            return True
        current = cls._current_model_tag()
        return current == published_model and current != "unknown"

    def _model_matches(self) -> bool:
        """Whether the live embeddings model is the one that produced the vectors.

        Cosine similarity between two different embedding spaces is a number
        without meaning, and nothing downstream can tell it apart from a real
        score -- which is exactly why the layer carries its ``model_tag``.
        An unknown tag on either side is not evidence of a match.
        """
        if self._model_tag is None:
            return True
        current = self._current_model_tag()
        if current == self._model_tag and current != "unknown":
            return True
        if not self._mismatch_reported:
            self._mismatch_reported = True
            logger.warning(
                f"VectorIndex: vectors were produced by {self._model_tag!r} but this "
                f"deployment embeds with {current!r}; refusing to score across "
                f"incompatible spaces. Rebuild the index locally to query it."
            )
        return False

    def _embed(self, texts: list[str]) -> list[list[float]] | None:
        embeddings = _get_embeddings()
        if embeddings is None:
            return None
        try:
            vecs = embeddings.embed_documents(texts)
        except Exception as exc:
            logger.warning(f"VectorIndex embedding failed: {exc}")
            return None
        if inspect.isawaitable(vecs):
            # An async connector would otherwise leak an un-awaited coroutine and
            # fail much later, inside zip() or on qvec_list[0].
            vecs.close()
            logger.warning(
                "VectorIndex: the configured embeddings provider is async; "
                "embed_documents must be callable synchronously here."
            )
            return None
        return vecs

    def build(self, blocks: Iterable[Block], content: ContentReader) -> None:
        """Embed every block, or raise.

        This index must never return normally without having built anything.
        ``Brain._rebuild`` vouches for a module as soon as ``build`` returns
        (``brain.py:1080-1083``) and ``_pack_index`` publishes a layer for any
        vouched module, so returning empty ships an artifact that claims a
        vector index and carries none -- which a consumer can neither detect
        nor rebuild. Failing here is what keeps the layer off the manifest.

        An empty *module* is a different thing and is allowed: an index with
        no blocks in it describes the composition accurately.
        """
        embeddings = _get_embeddings()
        if embeddings is None:
            self._vectors = {}
            self._model_tag = None
            raise DistributionError(
                "VectorIndex cannot be built: no embeddings model is configured. "
                "Register this index only where an embeddings provider is available, "
                "or the module would publish a vector-index layer carrying nothing."
            )

        self._model_tag = self._current_model_tag()
        self._mismatch_reported = False

        texts: list[str] = []
        bids: list[BlockId] = []
        for block in blocks:
            texts.append(_block_text(block, content))
            bids.append(block.block_id)

        if not texts:
            self._vectors = {}
            return

        vecs = self._embed(texts)
        if vecs is None:
            self._vectors = {}
            raise DistributionError(
                f"VectorIndex cannot be built: the embeddings call failed for "
                f"{len(texts)} block(s). See the logged cause."
            )

        if len(vecs) != len(bids):
            self._vectors = {}
            raise DistributionError(
                f"VectorIndex cannot be built: the embeddings provider returned "
                f"{len(vecs)} vector(s) for {len(bids)} block(s). A partial result "
                f"would publish a layer covering only some of the module."
            )

        self._vectors = dict(zip(bids, vecs, strict=True))

    def _cosine_similarity(self, a: list[float], b: list[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b, strict=False))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))
        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0
        return dot / (norm_a * norm_b)

    def search(self, query: Any, limit: int = 10) -> list[tuple[BlockId, float]]:
        if not self._vectors:
            return []
        if not isinstance(query, str):
            return []
        if not self._model_matches():
            return []
        qvec_list = self._embed([query])
        if qvec_list is None or not qvec_list:
            return []
        qvec = qvec_list[0]
        scores = {
            bid: self._cosine_similarity(qvec, vec)
            for bid, vec in self._vectors.items()
        }
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [(bid, float(_score_to_str(s))) for bid, s in ranked[:limit]]

    def dump(self) -> bytes:
        """Versioned, stable dump format."""
        data = {
            "version": self._DUMP_VERSION,
            "model_tag": self._model_tag,
            "vectors": {str(bid): vec for bid, vec in self._vectors.items()},
        }
        return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")

    def load(self, data: bytes) -> None:
        """Restore a travelling layer, checking it against the live model.

        A mismatch warns rather than raises: the rest of the brain -- blocks,
        structural indices -- is perfectly usable, and refusing the whole
        restore over one unusable index would cost more than it protects.
        ``search`` is where the refusal has to bite.
        """
        payload = json.loads(data.decode("utf-8"))
        version = payload.get("version")
        if version != self._DUMP_VERSION:
            raise ValueError(
                f"VectorIndex dump version mismatch: expected {self._DUMP_VERSION}, got {version}"
            )
        self._model_tag = payload.get("model_tag")
        self._vectors = {BlockId.parse(k): v for k, v in payload["vectors"].items()}
        self._mismatch_reported = False
        self._model_matches()


# --------------------------------------------------------------------------- #
# 5. Graph — neighbourhood / traversal
# --------------------------------------------------------------------------- #


class GraphIndex(AbstractIndex):
    """Adjacency-list graph index derived from block relations.

    Semantic blocks may declare ``relations`` (subject → predicate → object);
    this index treats each relation as a directed edge and supports
    neighbourhood traversal starting from a seed block.

    ``rebuildable = True``.
    """

    KIND: ClassVar[IndexKind] = IndexKind.GRAPH
    REBUILDABLE: ClassVar[bool] = True

    def __init__(self) -> None:
        self._adj: dict[BlockId, list[tuple[str, BlockId]]] = defaultdict(list)
        self._reverse: dict[BlockId, list[tuple[str, BlockId]]] = defaultdict(list)
        self._nodes: set[BlockId] = set()

    def build(self, blocks: Iterable[Block], content: ContentReader) -> None:
        self._adj = defaultdict(list)
        self._reverse = defaultdict(list)
        self._nodes = set()
        for block in blocks:
            self._nodes.add(block.block_id)
            payload = block.payload()
            if not isinstance(payload, dict):
                continue
            relations = payload.get("relations")
            if relations is None:
                continue
            for rel in relations:
                if not isinstance(rel, dict):
                    continue
                target = rel.get("target")
                predicate = rel.get("predicate", "related")
                if target is not None:
                    try:
                        tid = BlockId.parse(target)
                        self._adj[block.block_id].append((predicate, tid))
                        self._reverse[tid].append((predicate, block.block_id))
                    except Exception:
                        pass
        self._adj = dict(self._adj)
        self._reverse = dict(self._reverse)

    def search(self, query: Any, limit: int = 10) -> list[tuple[BlockId, float]]:
        """Query may be a BlockId (seed) or a dict with ``seed`` and optional ``predicate``."""
        seed: BlockId | None = None
        predicate: str | None = None
        depth = 1
        if isinstance(query, BlockId):
            seed = query
        elif isinstance(query, dict):
            raw_seed = query.get("seed")
            if isinstance(raw_seed, BlockId):
                seed = raw_seed
            elif isinstance(raw_seed, str):
                try:
                    seed = BlockId.parse(raw_seed)
                except Exception:
                    return []
            predicate = query.get("predicate")
            depth = query.get("depth", 1)
        if seed is None:
            return []

        visited: set[BlockId] = {seed}
        frontier: list[BlockId] = [seed]
        results: list[tuple[BlockId, float]] = []
        for d in range(depth):
            next_frontier: list[BlockId] = []
            for node in frontier:
                for pred, target in self._adj.get(node, []):
                    if predicate is not None and pred != predicate:
                        continue
                    if target not in visited:
                        visited.add(target)
                        next_frontier.append(target)
                        score = 1.0 / (d + 1)
                        results.append((target, score))
            frontier = next_frontier
            if not frontier:
                break
        # Deduplicate while preserving best score
        best: dict[BlockId, float] = {}
        for bid, sc in results:
            best[bid] = max(best.get(bid, 0.0), sc)
        ranked = sorted(best.items(), key=lambda x: x[1], reverse=True)
        return [(bid, float(_score_to_str(s))) for bid, s in ranked[:limit]]


# --------------------------------------------------------------------------- #
# 6. Bitmap — categorical filter
# --------------------------------------------------------------------------- #


class BitmapIndex(AbstractIndex):
    """Simple bitmap / set index for categorical membership.

    Each block is tagged with a category key (e.g. ``kind``, ``tags``);
    queries ask for exact category matches.

    ``rebuildable = True``.
    """

    KIND: ClassVar[IndexKind] = IndexKind.BITMAP
    REBUILDABLE: ClassVar[bool] = True

    def __init__(self) -> None:
        self._sets: dict[str, set[BlockId]] = defaultdict(set)
        self._all: set[BlockId] = set()

    def build(self, blocks: Iterable[Block], content: ContentReader) -> None:
        self._sets = defaultdict(set)
        self._all = set()
        for block in blocks:
            self._all.add(block.block_id)
            payload = block.payload()
            if not isinstance(payload, dict):
                continue
            # Collect categories
            categories: set[str] = set()
            kind = payload.get("kind")
            if kind is not None:
                categories.add(str(kind))
            tags = payload.get("tags")
            if isinstance(tags, list):
                for t in tags:
                    categories.add(str(t))
            label = payload.get("label")
            if label is not None:
                categories.add(str(label))
            subject = payload.get("subject")
            if subject is not None:
                categories.add(str(subject))
            for cat in categories:
                self._sets[cat].add(block.block_id)
        self._sets = dict(self._sets)

    def search(self, query: Any, limit: int = 10) -> list[tuple[BlockId, float]]:
        if isinstance(query, str):
            matches = self._sets.get(query, set())
            return [(bid, 1.0) for bid in sorted(matches, key=lambda b: b.hex)[:limit]]
        if isinstance(query, dict):
            # $and / $or style
            if "$and" in query:
                parts = query["$and"]
                sets = [self._sets.get(str(p), set()) for p in parts]
                if not sets:
                    return []
                matches = sets[0].copy()
                for s in sets[1:]:
                    matches &= s
                return [
                    (bid, 1.0) for bid in sorted(matches, key=lambda b: b.hex)[:limit]
                ]
            if "$or" in query:
                parts = query["$or"]
                matches: set[BlockId] = set()
                for p in parts:
                    matches |= self._sets.get(str(p), set())
                return [
                    (bid, 1.0) for bid in sorted(matches, key=lambda b: b.hex)[:limit]
                ]
            # Single key/value
            for _k, v in query.items():
                matches = self._sets.get(str(v), set())
                return [
                    (bid, 1.0) for bid in sorted(matches, key=lambda b: b.hex)[:limit]
                ]
        return []
