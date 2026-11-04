"""Kafka-shaped event bus with a deterministic in-memory implementation."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol


def build_envelope(event_type: str, payload: dict[str, Any], key: str | None = None) -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "schema_version": 1,
        "event_type": event_type,
        "key": key,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


class Bus(Protocol):
    def publish(self, topic: str, event_type: str, payload: dict[str, Any], key: str | None = None) -> dict[str, Any]: ...


@dataclass
class InMemoryBus:
    messages: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    def publish(self, topic: str, event_type: str, payload: dict[str, Any], key: str | None = None) -> dict[str, Any]:
        envelope = build_envelope(event_type, payload, key=key)
        self.messages.append((topic, envelope))
        return envelope


class KafkaBus:
    """Publishes JSON envelopes to Kafka; the client is imported lazily."""

    def __init__(self, bootstrap_servers: str, producer: Any | None = None) -> None:
        if producer is not None:
            self._producer = producer
            return
        from confluent_kafka import Producer

        self._producer = Producer({"bootstrap.servers": bootstrap_servers, "enable.idempotence": True})

    def publish(self, topic: str, event_type: str, payload: dict[str, Any], key: str | None = None) -> dict[str, Any]:
        envelope = build_envelope(event_type, payload, key=key)
        self._producer.produce(
            topic,
            key=key.encode() if key else None,
            value=json.dumps(envelope, separators=(",", ":")).encode(),
        )
        self._producer.poll(0)
        return envelope
