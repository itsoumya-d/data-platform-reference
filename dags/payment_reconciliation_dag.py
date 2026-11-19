"""Airflow DAG for Payment Lifecycle & PSP Settlement Reconciliation.

Automates daily ingestion of PSP settlement files, compares against internal
captured transaction ledgers, detects discrepancies, and updates analytical
reconciliation data marts.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from airflow.decorators import dag, task

PROJECT = Path(__file__).resolve().parents[1]


@dag(
    dag_id="payment_reconciliation_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["payments", "fintech", "reconciliation"],
)
def payment_reconciliation_dag():
    @task
    def fetch_ledger_transactions() -> list[dict]:
        """Fetches captured and settled transactions from the internal ledger."""
        return [
            {
                "event_id": "evt_001",
                "idempotency_key": "ik_001",
                "merchant_id": "m_payrails_01",
                "customer_id": "cust_101",
                "amount": 250.00,
                "currency": "EUR",
                "psp": "ADYEN",
                "status": "SETTLED",
                "processing_fee": 2.75,
                "psp_latency_ms": 175.0,
            },
            {
                "event_id": "evt_002",
                "idempotency_key": "ik_002",
                "merchant_id": "m_payrails_01",
                "customer_id": "cust_102",
                "amount": 89.50,
                "currency": "EUR",
                "psp": "STRIPE",
                "status": "CAPTURED",
                "processing_fee": 1.34,
                "psp_latency_ms": 210.0,
            },
        ]

    @task
    def fetch_psp_settlement_batch() -> list[dict]:
        """Ingests external processor settlement batch records."""
        return [
            {
                "settlement_id": "set_ady_901",
                "payment_id": "evt_001",
                "idempotency_key": "ik_001",
                "psp": "ADYEN",
                "gross_amount": 250.00,
                "net_amount": 247.25,
                "fee": 2.75,
            },
            {
                "settlement_id": "set_str_902",
                "payment_id": "evt_002",
                "idempotency_key": "ik_002",
                "psp": "STRIPE",
                "gross_amount": 89.50,
                "net_amount": 88.16,
                "fee": 1.34,
            },
        ]

    @task
    def run_reconciliation(ledger: list[dict], settlements: list[dict]) -> dict:
        """Executes reconciliation logic via PaymentReconciliationEngine."""
        from dataplat.payment_stream_processor import (
            Currency,
            PaymentEvent,
            PaymentReconciliationEngine,
            PaymentStatus,
            PSPProvider,
            SettlementRecord,
        )

        ledger_events = [
            PaymentEvent(
                event_id=item["event_id"],
                idempotency_key=item["idempotency_key"],
                merchant_id=item["merchant_id"],
                customer_id=item["customer_id"],
                amount=item["amount"],
                currency=Currency(item["currency"]),
                psp=PSPProvider(item["psp"]),
                status=PaymentStatus(item["status"]),
                processing_fee=item["processing_fee"],
                psp_latency_ms=item["psp_latency_ms"],
            )
            for item in ledger
        ]

        settlement_records = [
            SettlementRecord(
                settlement_id=s["settlement_id"],
                payment_id=s["payment_id"],
                idempotency_key=s["idempotency_key"],
                psp=PSPProvider(s["psp"]),
                gross_amount=s["gross_amount"],
                net_amount=s["net_amount"],
                fee=s["fee"],
            )
            for s in settlements
        ]

        report = PaymentReconciliationEngine.reconcile(ledger_events, settlement_records)
        return {
            "matched_count": report.matched_count,
            "discrepancy_count": report.discrepancy_count,
            "total_discrepancy_amount": report.total_discrepancy_amount,
            "is_clean": report.is_clean,
            "reconciled_at": datetime.now(UTC).isoformat(),
        }

    ledger_data = fetch_ledger_transactions()
    settle_data = fetch_psp_settlement_batch()
    run_reconciliation(ledger_data, settle_data)


payment_reconciliation_dag()
