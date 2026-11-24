from __future__ import annotations

from datetime import UTC, datetime, timedelta

from dataplat.banking_reconciliation import (
    DiscrepancyType,
    FinancialTransaction,
    LedgerReconciliationPipeline,
    PostingDirection,
    PostingLeg,
    StreamingLedgerMonitor,
    TransactionAuditEngine,
)


def _make_sample_tx(
    tx_id: str = "tx-001",
    idempotency_key: str = "idem-001",
    amount: float = 250.0,
    currency: str = "NZD",
    created_at: str | None = None,
) -> FinancialTransaction:
    now = created_at or datetime.now(UTC).isoformat()
    return FinancialTransaction(
        transaction_id=tx_id,
        idempotency_key=idempotency_key,
        legs=[
            PostingLeg(
                account_id="acc-debit-1",
                direction=PostingDirection.DEBIT,
                amount=amount,
                currency=currency,
                occurred_at=now,
            ),
            PostingLeg(
                account_id="acc-credit-1",
                direction=PostingDirection.CREDIT,
                amount=amount,
                currency=currency,
                occurred_at=now,
            ),
        ],
        created_at=now,
        metadata={"channel": "mobile_app", "transfer_type": "internal"},
    )


def test_balanced_double_entry_transaction():
    engine = TransactionAuditEngine()
    tx = _make_sample_tx()
    is_valid, err_msg, err_type = engine.validate_transaction(tx)
    assert is_valid is True
    assert err_msg is None
    assert err_type is None


def test_unbalanced_double_entry_transaction():
    engine = TransactionAuditEngine()
    now = datetime.now(UTC).isoformat()
    unbalanced_tx = FinancialTransaction(
        transaction_id="tx-unbalanced",
        idempotency_key="idem-unbalanced",
        legs=[
            PostingLeg("acc-1", PostingDirection.DEBIT, 100.0, "NZD", now),
            PostingLeg("acc-2", PostingDirection.CREDIT, 95.0, "NZD", now),
        ],
        created_at=now,
    )
    is_valid, err_msg, err_type = engine.validate_transaction(unbalanced_tx)
    assert is_valid is False
    assert err_type == DiscrepancyType.UNBALANCED_POSTING
    assert "Double-entry imbalance" in (err_msg or "")


def test_idempotency_collision_detection():
    engine = TransactionAuditEngine()
    tx1 = _make_sample_tx(tx_id="tx-001", idempotency_key="key-abc")
    is_valid1, _, _ = engine.validate_transaction(tx1)
    assert is_valid1 is True

    # Same key with different transaction id
    tx2 = _make_sample_tx(tx_id="tx-002", idempotency_key="key-abc")
    is_valid2, err_msg, err_type = engine.validate_transaction(tx2)
    assert is_valid2 is False
    assert err_type == DiscrepancyType.IDEMPOTENCY_COLLISION
    assert "collision" in (err_msg or "")


def test_idempotent_replay():
    engine = TransactionAuditEngine()
    tx = _make_sample_tx(tx_id="tx-001", idempotency_key="key-replay")
    is_valid1, _, _ = engine.validate_transaction(tx)
    assert is_valid1 is True

    # Same transaction replayed
    is_valid2, msg, _ = engine.validate_transaction(tx)
    assert is_valid2 is True
    assert "replay" in (msg or "")


def test_reconciliation_exact_match():
    pipeline = LedgerReconciliationPipeline()
    now_iso = datetime.now(UTC).isoformat()
    tx1 = _make_sample_tx(tx_id="tx-101", idempotency_key="key-101", amount=1500.0, created_at=now_iso)
    tx2 = _make_sample_tx(tx_id="tx-102", idempotency_key="key-102", amount=850.50, created_at=now_iso)

    settlements = [
        {"transaction_id": "tx-101", "settled_amount": 1500.0, "settled_at": now_iso},
        {"transaction_id": "tx-102", "settled_amount": 850.50, "settled_at": now_iso},
    ]

    summary = pipeline.reconcile([tx1, tx2], settlements)
    assert summary.total_transactions_processed == 2
    assert summary.matched_transactions == 2
    assert summary.unmatched_transactions == 0
    assert summary.total_discrepancies == 0
    assert summary.sla_compliance_rate == 100.0


def test_reconciliation_amount_mismatch():
    pipeline = LedgerReconciliationPipeline()
    now_iso = datetime.now(UTC).isoformat()
    tx = _make_sample_tx(tx_id="tx-diff", idempotency_key="key-diff", amount=100.0, created_at=now_iso)
    settlements = [
        {"transaction_id": "tx-diff", "settled_amount": 99.50, "settled_at": now_iso}
    ]

    summary = pipeline.reconcile([tx], settlements)
    assert summary.matched_transactions == 1
    assert summary.total_discrepancies == 1
    assert summary.discrepancies[0].discrepancy_type == DiscrepancyType.AMOUNT_MISMATCH


def test_reconciliation_timing_skew():
    pipeline = LedgerReconciliationPipeline(max_skew_seconds=60)
    tx_time = datetime.now(UTC)
    settle_time = tx_time + timedelta(seconds=120)

    tx = _make_sample_tx(
        tx_id="tx-skew",
        idempotency_key="key-skew",
        amount=50.0,
        created_at=tx_time.isoformat(),
    )
    settlements = [
        {"transaction_id": "tx-skew", "settled_amount": 50.0, "settled_at": settle_time.isoformat()}
    ]

    summary = pipeline.reconcile([tx], settlements)
    assert summary.total_discrepancies == 1
    assert summary.discrepancies[0].discrepancy_type == DiscrepancyType.SETTLEMENT_TIMING_SKEW


def test_orphan_settlement_detection():
    pipeline = LedgerReconciliationPipeline()
    now_iso = datetime.now(UTC).isoformat()
    settlements = [
        {"transaction_id": "tx-orphan-999", "settled_amount": 420.0, "settled_at": now_iso}
    ]

    summary = pipeline.reconcile([], settlements)
    assert summary.total_discrepancies == 1
    assert summary.discrepancies[0].discrepancy_type == DiscrepancyType.ORPHAN_SETTLEMENT


def test_streaming_ledger_monitor():
    monitor = StreamingLedgerMonitor(window_size=10, alert_threshold_pct=20.0)
    # First 8 successes
    for _ in range(8):
        assert monitor.record_match(True) is False
    # 2 failures -> 2/10 = 20% -> threshold breached
    monitor.record_match(False)
    alert = monitor.record_match(False)
    assert alert is True
