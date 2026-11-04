"""Retrieval-backed insight service with an extractive, citation-first provider."""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

TOKEN_RE = re.compile(r"[a-z0-9]+")
STOP = {
    "the", "a", "an", "and", "or", "for", "to", "of", "in", "on", "is", "was",
    "were", "it", "this", "that", "with", "as", "at", "by", "from", "be", "has",
}


def tokens(text: str) -> list[str]:
    return [t for t in TOKEN_RE.findall(text.lower()) if t not in STOP and len(t) > 1]


@dataclass(frozen=True)
class Note:
    note_id: str
    customer_id: str
    text: str


def load_notes(path: str | Path) -> list[Note]:
    import pandas as pd

    frame = pd.read_parquet(path)
    return [
        Note(str(row.note_id), str(row.customer_id), str(row.text))
        for row in frame.itertuples(index=False)
    ]


class TfidfRetriever:
    def __init__(self, notes: list[Note]) -> None:
        self.notes = notes
        self._doc_tokens = [tokens(note.text) for note in notes]
        document_frequency: Counter[str] = Counter()
        for doc in self._doc_tokens:
            document_frequency.update(set(doc))
        total = max(len(notes), 1)
        self._idf = {term: math.log((1 + total) / (1 + count)) + 1 for term, count in document_frequency.items()}
        self._vectors = [self._vector(doc) for doc in self._doc_tokens]
        self._norms = [math.sqrt(sum(value * value for value in vector.values())) or 1.0 for vector in self._vectors]

    def _vector(self, doc: list[str]) -> dict[str, float]:
        counts = Counter(doc)
        return {term: count * self._idf.get(term, 1.0) for term, count in counts.items()}

    def search(self, query: str, k: int = 3) -> list[Note]:
        query_vector = self._vector(tokens(query))
        query_norm = math.sqrt(sum(value * value for value in query_vector.values())) or 1.0
        scored: list[tuple[float, Note]] = []
        for vector, norm, note in zip(self._vectors, self._norms, self.notes):
            score = sum(value * vector.get(term, 0.0) for term, value in query_vector.items()) / (query_norm * norm)
            if score > 0:
                scored.append((score, note))
        scored.sort(key=lambda item: (-item[0], item[1].note_id))
        return [note for _, note in scored[:k]]


class ExtractiveProvider:
    name = "extractive-v1"

    def summarize(self, query: str, notes: list[Note]) -> dict[str, object]:
        query_terms = set(tokens(query))
        sentences: list[str] = []
        citations: list[str] = []
        for note in notes:
            candidates = [s.strip() for s in re.split(r"(?<=[.!?])\s+", note.text) if s.strip()]
            chosen = next(
                (sentence for sentence in candidates if query_terms & set(tokens(sentence))),
                candidates[0] if candidates else note.text,
            )
            sentences.append(chosen)
            citations.append(note.note_id)
        summary = " ".join(sentences) if sentences else "No supporting notes were found."
        return {"summary": summary, "citations": citations, "provider": self.name}


class InsightService:
    def __init__(self, notes: list[Note], provider: ExtractiveProvider | None = None) -> None:
        self.retriever = TfidfRetriever(notes)
        self.provider = provider or ExtractiveProvider()

    def insight(self, customer_id: str, query: str = "payment risk refund delay") -> dict[str, object]:
        customer_notes = [note for note in self.retriever.notes if note.customer_id == customer_id]
        if not customer_notes:
            return {"summary": "No notes for this customer.", "citations": [], "provider": self.provider.name}
        retrieved = TfidfRetriever(customer_notes).search(query, k=3)
        result = self.provider.summarize(query, retrieved)
        result["retrieved"] = len(retrieved)
        return result
