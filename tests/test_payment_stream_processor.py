"""Unit and integration tests for Payment Lifecycle & Stream Processing Engine."""

from __future__ import annotations

import pytest

from dataplat.kafka_bus import InMemoryBus
from dataplat.payment_stream_processor import (
    Currency,
    DynamicPaymentRouter,
    PaymentDataQualityValidator,
    PaymentEvent,
    PaymentReconciliationEngine,
    PaymentStatus,
    PaymentStreamProcessor,
    PaymentWarehouseExporter,
    PSPProvider,
    ReconciliationStatus,
    RoutingStrategy,
    SettlementRecord,
)


def test_data_quality_validation_and_gdpr_masking():
    event = PaymentEvent(
        event_id="evt_valid_1",
        idempotency_key="ik_valid_1",
        merchant_id="merchant_berlin_42",
        customer_id="cust_99",
        amount=149.99,
        currency=Currency.EUR,
        psp=PSPProvider.STRIPE,
        status=PaymentStatus.AUTHORIZED,
        metadata={"customer_note": "Card was 4532-1234-5678-9012 for test", "order_ref": "REF-8842"},
        masked_pan="4532123456789012",
    )

    sanitized = PaymentDataQualityValidator.validate_and_sanitize(event)
    assert sanitized.amount == 149.99
    assert sanitized.masked_pan == "453212******9012"
    assert "453212******9012" in sanitized.metadata["customer_note"]

    # Invariant: negative amount raises
    invalid_event = PaymentEvent(
        event_id="evt_bad_1",
        idempotency_key="ik_bad_1",
        merchant_id="merchant_berlin_42",
        customer_id="cust_99",
        amount=-50.0,
        currency=Currency.EUR,
        psp=PSPProvider.STRIPE,
        status=PaymentStatus.AUTHORIZED,
    )
    with pytest.raises(ValueError, match="strictly positive"):
        PaymentDataQualityValidator.validate_and_sanitize(invalid_event)

    # Invariant: empty idempotency key raises
    empty_key_event = PaymentEvent(
        event_id="evt_bad_2",
        idempotency_key="   ",
        merchant_id="merchant_berlin_42",
        customer_id="cust_99",
        amount=10.0,
        currency=Currency.EUR,
        psp=PSPProvider.STRIPE,
        status=PaymentStatus.AUTHORIZED,
    )
    with pytest.raises(ValueError, match="Idempotency key"):
        PaymentDataQualityValidator.validate_and_sanitize(empty_key_event)


def test_dynamic_payment_routing_strategies():
    router = DynamicPaymentRouter()

    # Strategy 1: Lowest fee for EUR selects INTERNAL (20 bps) or ADYEN (110 bps)
    eur_decision = router.route_transaction(
        payment_id="pay_001",
        amount=1000.0,
        currency=Currency.EUR,
        strategy=RoutingStrategy.LOWEST_FEE,
    )
    assert eur_decision.selected_psp == PSPProvider.INTERNAL
    assert eur_decision.estimated_fee == 2.0  # 1000 * 0.0020

    # Strategy 2: Lowest fee for USD (where INTERNAL is ineligible) selects ADYEN (110 bps) over STRIPE (150 bps)
    usd_decision = router.route_transaction(
        payment_id="pay_002",
        amount=1000.0,
        currency=Currency.USD,
        strategy=RoutingStrategy.LOWEST_FEE,
    )
    assert usd_decision.selected_psp == PSPProvider.ADYEN
    assert usd_decision.estimated_fee == 11.0  # 1000 * 0.0110

    # Strategy 3: Highest success rate selects ADYEN (99.0%)
    success_decision = router.route_transaction(
        payment_id="pay_003",
        amount=500.0,
        currency=Currency.USD,
        strategy=RoutingStrategy.HIGHEST_SUCCESS_RATE,
    )
    assert success_decision.selected_psp == PSPProvider.ADYEN


def test_payment_stream_processor_lifecycle_and_idempotency():
    bus = InMemoryBus()
    processor = PaymentStreamProcessor(bus=bus)

    auth_event = PaymentEvent(
        event_id="evt_auth_101",
        idempotency_key="ik_tx_8801",
        merchant_id="m_lux_01",
        customer_id="cust_8801",
        amount=320.00,
        currency=Currency.EUR,
        psp=PSPProvider.ADYEN,
        status=PaymentStatus.AUTHORIZED,
        processing_fee=3.52,
        psp_latency_ms=160.0,
    )

    # 1. First event processes cleanly
    ok, msg, current = processor.process_event(auth_event)
    assert ok is True
    assert msg == "PROCESSED_SUCCESSFULLY"
    assert current.status == PaymentStatus.AUTHORIZED
    assert len(bus.messages) == 1
    assert bus.messages[0][0] == "payments.lifecycle.events"
    assert bus.messages[0][1]["payload"]["status"] == "AUTHORIZED"

    # 2. Duplicate submission with exact same status is deduplicated idempotently
    dup_ok, dup_msg, _ = processor.process_event(auth_event)
    assert dup_ok is False
    assert dup_msg == "IDEMPOTENT_DUPLICATE_IGNORED"
    assert len(bus.messages) == 1

    # 3. Transition to CAPTURED
    capture_event = PaymentEvent(
        event_id="evt_cap_102",
        idempotency_key="ik_tx_8801",
        merchant_id="m_lux_01",
        customer_id="cust_8801",
        amount=320.00,
        currency=Currency.EUR,
        psp=PSPProvider.ADYEN,
        status=PaymentStatus.CAPTURED,
        processing_fee=3.52,
        psp_latency_ms=145.0,
    )
    cap_ok, cap_msg, cap_state = processor.process_event(capture_event)
    assert cap_ok is True
    assert cap_msg == "PROCESSED_SUCCESSFULLY"
    assert cap_state.status == PaymentStatus.CAPTURED
    assert len(bus.messages) == 2

    # 4. Transition to SETTLED
    settle_event = PaymentEvent(
        event_id="evt_set_103",
        idempotency_key="ik_tx_8801",
        merchant_id="m_lux_01",
        customer_id="cust_8801",
        amount=320.00,
        currency=Currency.EUR,
        psp=PSPProvider.ADYEN,
        status=PaymentStatus.SETTLED,
        processing_fee=3.52,
        psp_latency_ms=120.0,
    )
    set_ok, set_msg, set_state = processor.process_event(settle_event)
    assert set_ok is True
    assert set_msg == "PROCESSED_SUCCESSFULLY"
    assert set_state.status == PaymentStatus.SETTLED
    assert len(bus.messages) == 3


def test_payment_stream_processor_illegal_transition():
    processor = PaymentStreamProcessor()

    auth_event = PaymentEvent(
        event_id="evt_auth_201",
        idempotency_key="ik_tx_9901",
        merchant_id="m_lux_02",
        customer_id="cust_9901",
        amount=50.00,
        currency=Currency.USD,
        psp=PSPProvider.STRIPE,
        status=PaymentStatus.AUTHORIZED,
    )
    processor.process_event(auth_event)

    # Illegal transition: AUTHORIZED cannot jump directly to SETTLED without CAPTURED
    invalid_settle = PaymentEvent(
        event_id="evt_set_202",
        idempotency_key="ik_tx_9901",
        merchant_id="m_lux_02",
        customer_id="cust_9901",
        amount=50.00,
        currency=Currency.USD,
        psp=PSPProvider.STRIPE,
        status=PaymentStatus.SETTLED,
    )
    ok, msg, current = processor.process_event(invalid_settle)
    assert ok is False
    assert "ILLEGAL_STATE_TRANSITION" in msg
    assert current.status == PaymentStatus.AUTHORIZED


def test_reconciliation_engine_clean_and_discrepancies():
    # Matched pair
    event_1 = PaymentEvent(
        event_id="e_1",
        idempotency_key="ik_1",
        merchant_id="m_01",
        customer_id="c_01",
        amount=100.0,
        currency=Currency.EUR,
        psp=PSPProvider.ADYEN,
        status=PaymentStatus.SETTLED,
        processing_fee=1.10,
    )
    settle_1 = SettlementRecord(
        settlement_id="s_1",
        payment_id="e_1",
        idempotency_key="ik_1",
        psp=PSPProvider.ADYEN,
        gross_amount=100.0,
        net_amount=98.90,
        fee=1.10,
    )

    clean_report = PaymentReconciliationEngine.reconcile([event_1], [settle_1])
    assert clean_report.is_clean is True
    assert clean_report.matched_count == 1
    assert clean_report.discrepancy_count == 0

    # Unmatched processor record
    settle_unmatched = SettlementRecord(
        settlement_id="s_orphan",
        payment_id="e_orphan",
        idempotency_key="ik_orphan",
        psp=PSPProvider.STRIPE,
        gross_amount=45.0,
        net_amount=43.5,
        fee=1.5,
    )

    # Amount mismatch record
    event_mismatch = PaymentEvent(
        event_id="e_mismatch",
        idempotency_key="ik_mismatch",
        merchant_id="m_01",
        customer_id="c_02",
        amount=200.0,
        currency=Currency.EUR,
        psp=PSPProvider.CHECKOUT,
        status=PaymentStatus.SETTLED,
        processing_fee=2.40,
    )
    settle_mismatch = SettlementRecord(
        settlement_id="s_mismatch",
        payment_id="e_mismatch",
        idempotency_key="ik_mismatch",
        psp=PSPProvider.CHECKOUT,
        gross_amount=190.0,  # $10 discrepancy
        net_amount=187.6,
        fee=2.40,
    )

    mixed_report = PaymentReconciliationEngine.reconcile(
        [event_1, event_mismatch],
        [settle_1, settle_unmatched, settle_mismatch],
    )
    assert mixed_report.is_clean is False
    assert mixed_report.matched_count == 1
    assert mixed_report.discrepancy_count == 2
    assert mixed_report.total_discrepancy_amount == 55.0  # 45.0 + 10.0

    statuses = {d.status for d in mixed_report.discrepancies}
    assert ReconciliationStatus.UNMATCHED_LEDGER in statuses
    assert ReconciliationStatus.AMOUNT_MISMATCH in statuses


def test_warehouse_aggregation_and_snowflake_ddl():
    events = [
        PaymentEvent(
            event_id="e_1",
            idempotency_key="ik_1",
            merchant_id="m_01",
            customer_id="c_01",
            amount=500.0,
            currency=Currency.EUR,
            psp=PSPProvider.ADYEN,
            status=PaymentStatus.SETTLED,
            processing_fee=5.50,
            psp_latency_ms=150.0,
        ),
        PaymentEvent(
            event_id="e_2",
            idempotency_key="ik_2",
            merchant_id="m_01",
            customer_id="c_02",
            amount=200.0,
            currency=Currency.EUR,
            psp=PSPProvider.STRIPE,
            status=PaymentStatus.DECLINED,
            processing_fee=0.0,
            psp_latency_ms=300.0,
        ),
    ]

    metrics = PaymentWarehouseExporter.aggregate_metrics(events)
    assert metrics.total_volume == 700.0
    assert metrics.transaction_count == 2
    assert metrics.conversion_rate == 0.5  # 1 out of 2 successful
    assert metrics.total_fees == 5.50
    assert metrics.volume_by_psp["ADYEN"] == 500.0
    assert metrics.volume_by_psp["STRIPE"] == 200.0
    assert metrics.success_rate_by_psp["ADYEN"] == 1.0
    assert metrics.success_rate_by_psp["STRIPE"] == 0.0
    # Baseline 2.4% on 700.0 = 16.80. Total fees = 5.50 -> Savings = 11.30
    assert metrics.estimated_routing_savings == 11.30

    ddl = PaymentWarehouseExporter.generate_snowflake_ddl()
    assert "raw_payment_events" in ddl
    assert "daily_payment_reconciliation_summary" in ddl
    assert "CLUSTER BY (occurred_at, merchant_id, psp)" in ddl
