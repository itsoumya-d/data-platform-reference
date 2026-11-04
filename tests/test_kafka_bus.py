from dataplat.kafka_bus import InMemoryBus


def test_in_memory_bus_preserves_key_and_envelope():
    bus = InMemoryBus()
    envelope = bus.publish("orders", "order.created", {"order_id": "O1"}, key="O1")
    assert bus.messages[0][0] == "orders"
    assert envelope["schema_version"] == 1
    assert envelope["event_type"] == "order.created"
    assert envelope["key"] == "O1"
    assert envelope["payload"] == {"order_id": "O1"}
    assert envelope["event_id"]
