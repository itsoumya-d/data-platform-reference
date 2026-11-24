from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class PostingDirection(str, Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


class DiscrepancyType(str, Enum):
    UNBALANCED_POSTING = "UNBALANCED_POSTING"
    IDEMPOTENCY_COLLISION = "IDEMPOTENCY_COLLISION"
    SETTLEMENT_TIMING_SKEW = "SETTLEMENT_TIMING_SKEW"
    CURRENCY_MISMATCH = "CURRENCY_MISMATCH"
    ORPHAN_SETTLEMENT = "ORPHAN_SETTLEMENT"
    AMOUNT_MISMATCH = "AMOUNT_MISMATCH"


@dataclass
class PostingLeg:
    account_id: str
    direction: PostingDirection
    amount: float
    currency: str
    occurred_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "direction": self.direction.value,
            "amount": round(self.amount, 4),
            "currency": self.currency.upper(),
            "occurred_at": self.occurred_at,
        }


@dataclass
class FinancialTransaction:
    transaction_id: str
    idempotency_key: str
    legs: list[PostingLeg]
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def compute_audit_hash(self) -> str:
        serialized_legs = json.dumps(
            [leg.to_dict() for leg in self.legs], sort_keys=True
        )
        payload = f"{self.transaction_id}:{self.idempotency_key}:{serialized_legs}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass
class ReconciliationDiscrepancy:
    discrepancy_type: DiscrepancyType
    transaction_id: str
    description: str
    detected_at: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReconciliationSummary:
    total_transactions_processed: int
    matched_transactions: int
    unmatched_transactions: int
    total_discrepancies: int
    net_reconciled_volume: dict[str, float]
    sla_compliance_rate: float
    discrepancies: list[ReconciliationDiscrepancy]


class TransactionAuditEngine:
    """Validates double-entry transactional integrity and idempotency."""

    def __init__(self, amount_tolerance: float = 0.0001) -> None:
        self.amount_tolerance = amount_tolerance
        self._processed_idempotency_keys: dict[str, str] = {}
        self._audit_trail: list[dict[str, Any]] = []

    def validate_transaction(
        self, tx: FinancialTransaction
    ) -> tuple[bool, str | None, DiscrepancyType | None]:
        if not tx.legs:
            return False, "Transaction contains no posting legs", DiscrepancyType.UNBALANCED_POSTING

        # Check idempotency
        if tx.idempotency_key in self._processed_idempotency_keys:
            existing_tx_id = self._processed_idempotency_keys[tx.idempotency_key]
            if existing_tx_id != tx.transaction_id:
                return (
                    False,
                    f"Idempotency key '{tx.idempotency_key}' collision with existing transaction '{existing_tx_id}'",
                    DiscrepancyType.IDEMPOTENCY_COLLISION,
                )
            return True, "Idempotent replay detected", None

        # Double-entry balance check grouped by currency
        balances_by_currency: dict[str, float] = {}
        for leg in tx.legs:
            curr = leg.currency.upper()
            if leg.amount <= 0:
                return (
                    False,
                    f"Invalid non-positive amount {leg.amount} on account {leg.account_id}",
                    DiscrepancyType.UNBALANCED_POSTING,
                )

            net_delta = leg.amount if leg.direction == PostingDirection.CREDIT else -leg.amount
            balances_by_currency[curr] = balances_by_currency.get(curr, 0.0) + net_delta

        for curr, balance in balances_by_currency.items():
            if abs(balance) > self.amount_tolerance:
                return (
                    False,
                    f"Double-entry imbalance in currency {curr}: net delta {balance:.4f}",
                    DiscrepancyType.UNBALANCED_POSTING,
                )

        audit_hash = tx.compute_audit_hash()
        self._processed_idempotency_keys[tx.idempotency_key] = tx.transaction_id
        self._audit_trail.append({
            "transaction_id": tx.transaction_id,
            "idempotency_key": tx.idempotency_key,
            "audit_hash": audit_hash,
            "timestamp": datetime.now(UTC).isoformat(),
        })

        return True, None, None


class LedgerReconciliationPipeline:
    """High-throughput multi-tier reconciliation engine comparing source events and settlements."""

    def __init__(
        self,
        max_skew_seconds: int = 3600,
        amount_tolerance: float = 0.01,
    ) -> None:
        self.max_skew_seconds = max_skew_seconds
        self.amount_tolerance = amount_tolerance
        self.audit_engine = TransactionAuditEngine(amount_tolerance=amount_tolerance)

    def reconcile(
        self,
        source_transactions: list[FinancialTransaction],
        settlement_records: list[dict[str, Any]],
    ) -> ReconciliationSummary:
        settlements_by_tx: dict[str, dict[str, Any]] = {
            r["transaction_id"]: r for r in settlement_records if "transaction_id" in r
        }
        matched_tx_ids: set[str] = set()
        discrepancies: list[ReconciliationDiscrepancy] = []
        volume_by_currency: dict[str, float] = {}
        total_processed = len(source_transactions)

        # Audit and match source transactions
        for tx in source_transactions:
            now_iso = datetime.now(UTC).isoformat()
            is_valid, error_msg, err_type = self.audit_engine.validate_transaction(tx)
            if not is_valid and err_type:
                discrepancies.append(
                    ReconciliationDiscrepancy(
                        discrepancy_type=err_type,
                        transaction_id=tx.transaction_id,
                        description=error_msg or "Validation failed",
                        detected_at=now_iso,
                        details={"metadata": tx.metadata},
                    )
                )
                continue

            if tx.transaction_id not in settlements_by_tx:
                discrepancies.append(
                    ReconciliationDiscrepancy(
                        discrepancy_type=DiscrepancyType.AMOUNT_MISMATCH,
                        transaction_id=tx.transaction_id,
                        description="Settlement record missing for source transaction",
                        detected_at=now_iso,
                    )
                )
                continue

            settlement = settlements_by_tx[tx.transaction_id]
            matched_tx_ids.add(tx.transaction_id)

            # Reconcile settlement leg amounts
            total_source_volume = sum(
                leg.amount for leg in tx.legs if leg.direction == PostingDirection.CREDIT
            )
            settlement_amount = float(settlement.get("settled_amount", 0.0))

            if abs(total_source_volume - settlement_amount) > self.amount_tolerance:
                discrepancies.append(
                    ReconciliationDiscrepancy(
                        discrepancy_type=DiscrepancyType.AMOUNT_MISMATCH,
                        transaction_id=tx.transaction_id,
                        description=(
                            f"Amount variance: source volume {total_source_volume:.2f} "
                            f"vs settled {settlement_amount:.2f}"
                        ),
                        detected_at=now_iso,
                        details={"expected": total_source_volume, "actual": settlement_amount},
                    )
                )

            # Reconcile timing skew
            try:
                tx_time = datetime.fromisoformat(tx.created_at)
                settle_time = datetime.fromisoformat(
                    settlement.get("settled_at", tx.created_at)
                )
                skew = abs((settle_time - tx_time).total_seconds())
                if skew > self.max_skew_seconds:
                    discrepancies.append(
                        ReconciliationDiscrepancy(
                            discrepancy_type=DiscrepancyType.SETTLEMENT_TIMING_SKEW,
                            transaction_id=tx.transaction_id,
                            description=f"Settlement timing skew exceeded: {skew:.0f}s > {self.max_skew_seconds}s",
                            detected_at=now_iso,
                            details={"skew_seconds": skew},
                        )
                    )
            except (ValueError, TypeError) as exc:
                logger.debug("Failed parsing timestamp for timing skew: %s", exc)

            for leg in tx.legs:
                curr = leg.currency.upper()
                volume_by_currency[curr] = volume_by_currency.get(curr, 0.0) + leg.amount

        # Check for orphan settlements (records in settlement not present in sources)
        for tx_id, settlement in settlements_by_tx.items():
            if tx_id not in matched_tx_ids:
                discrepancies.append(
                    ReconciliationDiscrepancy(
                        discrepancy_type=DiscrepancyType.ORPHAN_SETTLEMENT,
                        transaction_id=tx_id,
                        description=f"Orphan settlement without matching source transaction: {tx_id}",
                        detected_at=datetime.now(UTC).isoformat(),
                        details=settlement,
                    )
                )

        matched_count = len(matched_tx_ids)
        unmatched_count = total_processed - matched_count
        sla_rate = (matched_count / total_processed * 100.0) if total_processed > 0 else 100.0

        return ReconciliationSummary(
            total_transactions_processed=total_processed,
            matched_transactions=matched_count,
            unmatched_transactions=unmatched_count,
            total_discrepancies=len(discrepancies),
            net_reconciled_volume={k: round(v, 2) for k, v in volume_by_currency.items()},
            sla_compliance_rate=round(sla_rate, 2),
            discrepancies=discrepancies,
        )


class StreamingLedgerMonitor:
    """Sliding-window monitor for streaming reconciliation and SLA breach detection."""

    def __init__(self, window_size: int = 100, alert_threshold_pct: float = 5.0) -> None:
        self.window_size = window_size
        self.alert_threshold_pct = alert_threshold_pct
        self._history: list[bool] = []

    def record_match(self, success: bool) -> bool:
        """Returns True if anomaly alert threshold is breached."""
        self._history.append(success)
        if len(self._history) > self.window_size:
            self._history.pop(0)

        failure_count = sum(1 for m in self._history if not m)
        failure_rate = (failure_count / len(self._history)) * 100.0
        return failure_rate >= self.alert_threshold_pct
