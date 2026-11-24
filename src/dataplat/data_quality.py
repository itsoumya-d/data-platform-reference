"""Automated streaming data quality gates, contract validation, and observability engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class QualityRule:
    rule_id: str
    dimension: str  # completeness, validity, uniqueness, freshness, schema
    description: str
    assertion_type: str
    target_field: str
    threshold: float = 1.0  # Min passing ratio (0.0 to 1.0)
    parameters: dict[str, Any] = field(default_factory=dict)


@dataclass
class RuleEvaluationResult:
    rule_id: str
    dimension: str
    passed: bool
    observed_score: float
    failed_count: int
    total_evaluated: int
    details: str = ""


@dataclass
class QualitySuiteReport:
    evaluated_at: str
    total_rules: int
    passed_rules: int
    failed_rules: int
    data_health_index: float  # Weighted or mean score 0.0 to 1.0
    results: list[RuleEvaluationResult] = field(default_factory=list)

    @property
    def is_healthy(self) -> bool:
        return self.failed_rules == 0 and self.data_health_index >= 0.95


class StreamingQualityGate:
    """Evaluates real-time streaming event envelopes against data contracts and quality rules."""

    def __init__(self, expected_schema_version: int = 1, max_freshness_seconds: int = 300) -> None:
        self.expected_schema_version = expected_schema_version
        self.max_freshness_seconds = max_freshness_seconds
        self.quarantined_events: list[tuple[dict[str, Any], str]] = []
        self.valid_event_count: int = 0
        self.rejected_event_count: int = 0

    def validate_envelope(self, envelope: dict[str, Any]) -> tuple[bool, str]:
        """Validates envelope structure, schema version, required headers, and timestamp freshness."""
        required_keys = {"event_id", "schema_version", "event_type", "occurred_at", "payload"}
        missing = required_keys - set(envelope.keys())
        if missing:
            reason = f"Missing required envelope keys: {sorted(missing)}"
            self.quarantined_events.append((envelope, reason))
            self.rejected_event_count += 1
            return False, reason

        if envelope["schema_version"] != self.expected_schema_version:
            reason = f"Schema version mismatch: expected {self.expected_schema_version}, got {envelope['schema_version']}"
            self.quarantined_events.append((envelope, reason))
            self.rejected_event_count += 1
            return False, reason

        # Freshness verification
        try:
            occurred_at = datetime.fromisoformat(envelope["occurred_at"])
            if occurred_at.tzinfo is None:
                occurred_at = occurred_at.replace(tzinfo=UTC)
            now = datetime.now(UTC)
            age = (now - occurred_at).total_seconds()
            if age > self.max_freshness_seconds:
                reason = f"Event freshness exceeded SLA: age={age:.1f}s > limit={self.max_freshness_seconds}s"
                self.quarantined_events.append((envelope, reason))
                self.rejected_event_count += 1
                return False, reason
            if age < -60:  # Allow slight clock skew, reject far-future events
                reason = f"Event timestamp is in the future: age={age:.1f}s"
                self.quarantined_events.append((envelope, reason))
                self.rejected_event_count += 1
                return False, reason
        except (ValueError, TypeError) as exc:
            reason = f"Invalid timestamp format in occurred_at: {exc}"
            self.quarantined_events.append((envelope, reason))
            self.rejected_event_count += 1
            return False, reason

        payload = envelope.get("payload")
        if not isinstance(payload, dict) or not payload:
            reason = "Payload must be a non-empty mapping"
            self.quarantined_events.append((envelope, reason))
            self.rejected_event_count += 1
            return False, reason

        self.valid_event_count += 1
        return True, "Valid envelope"

    def filter_stream(self, stream: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filters a stream of events, routing non-compliant records to dead-letter quarantine."""
        return [evt for evt in stream if self.validate_envelope(evt)[0]]


class ObservabilityEngine:
    """Computes dataset quality dimensions (completeness, uniqueness, validity, distribution) at scale."""

    def __init__(self, rules: list[QualityRule] | None = None) -> None:
        self.rules: list[QualityRule] = rules or []

    def add_rule(self, rule: QualityRule) -> None:
        self.rules.append(rule)

    def evaluate_records(self, records: list[dict[str, Any]]) -> QualitySuiteReport:
        if not records:
            return QualitySuiteReport(
                evaluated_at=datetime.now(UTC).isoformat(),
                total_rules=len(self.rules),
                passed_rules=0,
                failed_rules=len(self.rules),
                data_health_index=0.0,
                results=[],
            )

        total_evaluated = len(records)
        eval_results: list[RuleEvaluationResult] = []

        for rule in self.rules:
            result = self._evaluate_single_rule(rule, records, total_evaluated)
            eval_results.append(result)

        passed_count = sum(1 for r in eval_results if r.passed)
        failed_count = len(eval_results) - passed_count
        mean_score = sum(r.observed_score for r in eval_results) / len(eval_results) if eval_results else 1.0

        return QualitySuiteReport(
            evaluated_at=datetime.now(UTC).isoformat(),
            total_rules=len(eval_results),
            passed_rules=passed_count,
            failed_rules=failed_count,
            data_health_index=round(mean_score, 4),
            results=eval_results,
        )

    def _evaluate_single_rule(
        self, rule: QualityRule, records: list[dict[str, Any]], total: int
    ) -> RuleEvaluationResult:
        field = rule.target_field
        failed_count = 0

        if rule.assertion_type == "expect_not_null":
            failed_count = sum(1 for r in records if r.get(field) is None or str(r.get(field)).strip() == "")
        elif rule.assertion_type == "expect_values_between":
            min_v = rule.parameters.get("min")
            max_v = rule.parameters.get("max")
            for r in records:
                val = r.get(field)
                if val is None or (min_v is not None and val < min_v) or (max_v is not None and val > max_v):
                    failed_count += 1
        elif rule.assertion_type == "expect_unique":
            seen: set[Any] = set()
            duplicates = 0
            for r in records:
                val = r.get(field)
                if val in seen:
                    duplicates += 1
                else:
                    seen.add(val)
            failed_count = duplicates
        elif rule.assertion_type == "expect_regex":
            import re
            pattern = re.compile(rule.parameters.get("pattern", ".*"))
            failed_count = sum(1 for r in records if not pattern.match(str(r.get(field, ""))))
        elif rule.assertion_type == "expect_in_set":
            allowed = set(rule.parameters.get("allowed", []))
            failed_count = sum(1 for r in records if r.get(field) not in allowed)
        else:
            failed_count = 0

        observed_score = max(0.0, (total - failed_count) / total)
        passed = observed_score >= rule.threshold

        return RuleEvaluationResult(
            rule_id=rule.rule_id,
            dimension=rule.dimension,
            passed=passed,
            observed_score=round(observed_score, 4),
            failed_count=failed_count,
            total_evaluated=total,
            details=f"Passed {total - failed_count}/{total} records ({observed_score * 100:.1f}%) vs threshold {rule.threshold * 100:.1f}%",
        )


class DriftAnomalyMonitor:
    """Statistical anomaly detector for real-time drift detection across streaming payload metrics."""

    def __init__(self, baseline_window_size: int = 50, z_score_threshold: float = 3.0) -> None:
        self.baseline_window_size = baseline_window_size
        self.z_score_threshold = z_score_threshold
        self._history: dict[str, list[float]] = {}

    def observe_metric(self, metric_name: str, value: float) -> tuple[bool, float]:
        """Ingests a metric observation. Returns (is_anomaly, calculated_z_score)."""
        history = self._history.setdefault(metric_name, [])
        if len(history) < 10:
            history.append(value)
            return False, 0.0

        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        std_dev = variance ** 0.5

        history.append(value)
        if len(history) > self.baseline_window_size:
            self._history[metric_name] = history[-self.baseline_window_size :]

        if std_dev == 0.0:
            is_anomaly = value != mean
            return is_anomaly, 99.0 if is_anomaly else 0.0

        z_score = abs(value - mean) / std_dev
        is_anomaly = z_score >= self.z_score_threshold
        return is_anomaly, round(z_score, 2)
