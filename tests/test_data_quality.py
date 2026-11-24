from __future__ import annotations

from datetime import UTC, datetime, timedelta

from dataplat.data_quality import (
    DriftAnomalyMonitor,
    ObservabilityEngine,
    QualityRule,
    StreamingQualityGate,
)


def test_streaming_quality_gate_valid_envelope():
    gate = StreamingQualityGate(expected_schema_version=1, max_freshness_seconds=300)
    now_iso = datetime.now(UTC).isoformat()
    envelope = {
        "event_id": "evt-123",
        "schema_version": 1,
        "event_type": "telemetry.metric_recorded",
        "occurred_at": now_iso,
        "payload": {"sensor_id": "s-42", "reading": 98.6},
    }
    is_valid, reason = gate.validate_envelope(envelope)
    assert is_valid is True
    assert reason == "Valid envelope"
    assert gate.valid_event_count == 1
    assert gate.rejected_event_count == 0


def test_streaming_quality_gate_missing_keys():
    gate = StreamingQualityGate()
    envelope = {
        "event_id": "evt-123",
        "schema_version": 1,
    }
    is_valid, reason = gate.validate_envelope(envelope)
    assert is_valid is False
    assert "Missing required envelope keys" in reason
    assert gate.rejected_event_count == 1
    assert len(gate.quarantined_events) == 1


def test_streaming_quality_gate_schema_version_mismatch():
    gate = StreamingQualityGate(expected_schema_version=2)
    now_iso = datetime.now(UTC).isoformat()
    envelope = {
        "event_id": "evt-123",
        "schema_version": 1,
        "event_type": "test",
        "occurred_at": now_iso,
        "payload": {"data": 1},
    }
    is_valid, reason = gate.validate_envelope(envelope)
    assert is_valid is False
    assert "Schema version mismatch" in reason


def test_streaming_quality_gate_stale_event():
    gate = StreamingQualityGate(max_freshness_seconds=60)
    old_time = (datetime.now(UTC) - timedelta(seconds=120)).isoformat()
    envelope = {
        "event_id": "evt-old",
        "schema_version": 1,
        "event_type": "test",
        "occurred_at": old_time,
        "payload": {"data": 1},
    }
    is_valid, reason = gate.validate_envelope(envelope)
    assert is_valid is False
    assert "Event freshness exceeded SLA" in reason


def test_streaming_quality_gate_empty_payload():
    gate = StreamingQualityGate()
    now_iso = datetime.now(UTC).isoformat()
    envelope = {
        "event_id": "evt-empty",
        "schema_version": 1,
        "event_type": "test",
        "occurred_at": now_iso,
        "payload": {},
    }
    is_valid, reason = gate.validate_envelope(envelope)
    assert is_valid is False
    assert "Payload must be a non-empty mapping" in reason


def test_streaming_quality_gate_filter_stream():
    gate = StreamingQualityGate()
    now_iso = datetime.now(UTC).isoformat()
    stream = [
        {
            "event_id": f"evt-{i}",
            "schema_version": 1,
            "event_type": "event",
            "occurred_at": now_iso,
            "payload": {"idx": i},
        }
        for i in range(5)
    ]
    # Add corrupted event
    stream.append({"corrupted": True})

    filtered = gate.filter_stream(stream)
    assert len(filtered) == 5
    assert gate.valid_event_count == 5
    assert gate.rejected_event_count == 1


def test_observability_engine_evaluation():
    engine = ObservabilityEngine(
        rules=[
            QualityRule(
                rule_id="r1_not_null",
                dimension="completeness",
                description="User ID must not be null",
                assertion_type="expect_not_null",
                target_field="user_id",
                threshold=0.9,
            ),
            QualityRule(
                rule_id="r2_score_range",
                dimension="validity",
                description="Score between 0 and 100",
                assertion_type="expect_values_between",
                target_field="score",
                threshold=1.0,
                parameters={"min": 0, "max": 100},
            ),
            QualityRule(
                rule_id="r3_unique_id",
                dimension="uniqueness",
                description="User IDs must be unique",
                assertion_type="expect_unique",
                target_field="user_id",
                threshold=1.0,
            ),
            QualityRule(
                rule_id="r4_status_enum",
                dimension="validity",
                description="Status must be in allowed set",
                assertion_type="expect_in_set",
                target_field="status",
                threshold=1.0,
                parameters={"allowed": ["ACTIVE", "INACTIVE", "PENDING"]},
            ),
        ]
    )

    records = [
        {"user_id": "u1", "score": 85, "status": "ACTIVE"},
        {"user_id": "u2", "score": 92, "status": "INACTIVE"},
        {"user_id": "u3", "score": 78, "status": "PENDING"},
        {"user_id": "u4", "score": 64, "status": "ACTIVE"},
    ]

    report = engine.evaluate_records(records)
    assert report.total_rules == 4
    assert report.passed_rules == 4
    assert report.failed_rules == 0
    assert report.data_health_index == 1.0
    assert report.is_healthy is True


def test_observability_engine_failures():
    engine = ObservabilityEngine(
        rules=[
            QualityRule(
                rule_id="r_unique",
                dimension="uniqueness",
                description="Must be unique",
                assertion_type="expect_unique",
                target_field="id",
                threshold=1.0,
            ),
            QualityRule(
                rule_id="r_regex",
                dimension="validity",
                description="Code matches format",
                assertion_type="expect_regex",
                target_field="code",
                threshold=1.0,
                parameters={"pattern": r"^[A-Z]{3}-\d{3}$"},
            ),
        ]
    )

    records = [
        {"id": 1, "code": "ABC-123"},
        {"id": 1, "code": "INVALID_CODE"},  # Duplicate id, invalid code
    ]

    report = engine.evaluate_records(records)
    assert report.passed_rules == 0
    assert report.failed_rules == 2
    assert report.is_healthy is False


def test_drift_anomaly_monitor():
    monitor = DriftAnomalyMonitor(baseline_window_size=30, z_score_threshold=3.0)

    # Warm up with steady metrics around 100.0
    for _ in range(20):
        is_anomaly, z = monitor.observe_metric("latency_ms", 100.0)
        assert is_anomaly is False

    # Moderate variation should not trigger
    is_anomaly, z = monitor.observe_metric("latency_ms", 105.0)
    # std_dev was 0 for identical values, so any change with zero variance handled
    assert z > 0

    # Add diverse baseline
    monitor2 = DriftAnomalyMonitor()
    for val in [95.0, 98.0, 102.0, 101.0, 99.0, 100.0, 104.0, 96.0, 103.0, 97.0, 100.0]:
        monitor2.observe_metric("throughput", val)

    # Normal value
    is_anomaly, z = monitor2.observe_metric("throughput", 101.0)
    assert is_anomaly is False
    assert z < 3.0

    # Severe spike (outlier)
    is_anomaly, z = monitor2.observe_metric("throughput", 350.0)
    assert is_anomaly is True
    assert z >= 3.0
