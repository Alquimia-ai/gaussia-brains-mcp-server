"""Word-level text helpers for the search engines.

Ported from ``alquimia.core.text`` unchanged: the inverted index must tokenize
exactly as core does, or the same query ranks differently here (top-k parity).
"""

from __future__ import annotations

import re
import unicodedata

WORD = re.compile(r"[^\W_]+")


def fold(term: str) -> str:
    """*term* with its diacritics removed (NFKD, combining marks dropped)."""
    decomposed = unicodedata.normalize("NFKD", term)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def tokenize(text: str) -> list[str]:
    """Lower-cased words as written, diacritics included.

    Unicode-aware, so a word with non-ASCII letters is one token rather than
    being split at them. Text is NFC-normalized first, so precomposed and
    decomposed forms of the same character produce the same token.
    """
    text = unicodedata.normalize("NFC", text).lower()
    return [t for t in WORD.findall(text) if len(t) > 1]
