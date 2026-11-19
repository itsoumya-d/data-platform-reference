"""Payment Lifecycle & Stream Processing Engine for High-Throughput Fintech Platforms.

Provides real-time payment event streaming, multi-PSP routing optimization,
idempotent transaction lifecycle state machines, automated settlement reconciliation,
data quality contracts with GDPR masking, and analytical warehouse aggregation.
"""

from __future__ import annotations

import enum
import hashlib
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar

from dataplat.kafka_bus import Bus, InMemoryBus


class PaymentStatus(enum.Enum):
    AUTHORIZED = "AUTHORIZED"
    CAPTURED = "CAPTURED"
    SETTLED = "SETTLED"
    DECLINED = "DECLINED"
    REFUNDED = "REFUNDED"
    DISPUTED = "DISPUTED"


class PSPProvider(enum.Enum):
    STRIPE = "STRIPE"
    ADYEN = "ADYEN"
    CHECKOUT = "CHECKOUT"
    PAYPAL = "PAYPAL"
    INTERNAL = "INTERNAL"


class Currency(enum.Enum):
    EUR = "EUR"
    USD = "USD"
    GBP = "GBP"
    AUD = "AUD"
    NZD = "NZD"


class ReconciliationStatus(enum.Enum):
    MATCHED = "MATCHED"
    UNMATCHED_PROCESSOR = "UNMATCHED_PROCESSOR"
    UNMATCHED_LEDGER = "UNMATCHED_LEDGER"
    AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
    FEE_DISCREPANCY = "FEE_DISCREPANCY"


class RoutingStrategy(enum.Enum):
    LOWEST_FEE = "LOWEST_FEE"
    HIGHEST_SUCCESS_RATE = "HIGHEST_SUCCESS_RATE"
    GEO_LOCAL = "GEO_LOCAL"
    BALANCED = "BALANCED"


@dataclass
class PaymentEvent:
    event_id: str
    idempotency_key: str
    merchant_id: str
    customer_id: str
    amount: float
    currency: Currency
    psp: PSPProvider
    status: PaymentStatus
    processing_fee: float = 0.0
    psp_latency_ms: float = 0.0
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = field(default_factory=dict)
    masked_pan: str | None = None


@dataclass
class RoutingDecision:
    payment_id: str
    selected_psp: PSPProvider
    alternative_psps: list[PSPProvider]
    strategy_used: RoutingStrategy
    estimated_fee: float
    expected_latency_ms: float


@dataclass
class SettlementRecord:
    settlement_id: str
    payment_id: str
    idempotency_key: str
    psp: PSPProvider
    gross_amount: float
    net_amount: float
    fee: float
    settled_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class DiscrepancyItem:
    payment_id: str
    status: ReconciliationStatus
    ledger_amount: float | None
    processor_amount: float | None
    difference: float
    reason: str


@dataclass
class ReconciliationReport:
    total_ledger_records: int
    total_processor_records: int
    matched_count: int
    discrepancy_count: int
    total_discrepancy_amount: float
    discrepancies: list[DiscrepancyItem] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return self.discrepancy_count == 0


@dataclass
class AnalyticalMetrics:
    total_volume: float
    transaction_count: int
    conversion_rate: float
    total_fees: float
    p95_latency_ms: float
    volume_by_psp: dict[str, float]
    success_rate_by_psp: dict[str, float]
    estimated_routing_savings: float


class PaymentDataQualityValidator:
    """Enforces schema invariants, monetary constraints, and GDPR data masking."""

    PAN_PATTERN = re.compile(r"\b(?:\d[ -]*?){13,16}\b")

    @classmethod
    def mask_pan(cls, text: str) -> str:
        """Masks primary account numbers (PAN), preserving only first 6 and last 4 digits."""
        def _repl(match: re.Match[str]) -> str:
            raw = re.sub(r"[ -]", "", match.group(0))
            if len(raw) < 13 or len(raw) > 19:
                return match.group(0)
            return raw[:6] + "*" * (len(raw) - 10) + raw[-4:]

        return cls.PAN_PATTERN.sub(_repl, text)

    @classmethod
    def validate_and_sanitize(cls, event: PaymentEvent) -> PaymentEvent:
        if event.amount <= 0:
            raise ValueError(f"Invalid payment amount: {event.amount}. Must be strictly positive.")
        if not event.idempotency_key or len(event.idempotency_key.strip()) == 0:
            raise ValueError("Idempotency key must not be empty.")
        if not event.merchant_id:
            raise ValueError("Merchant ID is required.")

        # Sanitize metadata for GDPR compliance
        sanitized_meta = {}
        for k, v in event.metadata.items():
            if isinstance(v, str):
                sanitized_meta[k] = cls.mask_pan(v)
            else:
                sanitized_meta[k] = v

        masked_pan = event.masked_pan
        if masked_pan and len(masked_pan) > 10:
            clean_digits = re.sub(r"[ -]", "", masked_pan)
            masked_pan = clean_digits[:6] + "*" * (len(clean_digits) - 10) + clean_digits[-4:]

        event.metadata = sanitized_meta
        event.masked_pan = masked_pan
        return event


class DynamicPaymentRouter:
    """Calculates optimal PSP routing based on historical fees, success rates, and latency."""

    def __init__(self, psp_fee_bps: dict[PSPProvider, float] | None = None) -> None:
        # Basis points fee per PSP (e.g. 150 bps = 1.5%)
        self.psp_fee_bps = psp_fee_bps or {
            PSPProvider.STRIPE: 150.0,
            PSPProvider.ADYEN: 110.0,
            PSPProvider.CHECKOUT: 120.0,
            PSPProvider.PAYPAL: 240.0,
            PSPProvider.INTERNAL: 20.0,
        }
        self.success_rates = {
            PSPProvider.STRIPE: 0.985,
            PSPProvider.ADYEN: 0.990,
            PSPProvider.CHECKOUT: 0.982,
            PSPProvider.PAYPAL: 0.970,
            PSPProvider.INTERNAL: 0.999,
        }
        self.expected_latencies = {
            PSPProvider.STRIPE: 220.0,
            PSPProvider.ADYEN: 180.0,
            PSPProvider.CHECKOUT: 205.0,
            PSPProvider.PAYPAL: 450.0,
            PSPProvider.INTERNAL: 40.0,
        }

    def route_transaction(
        self,
        payment_id: str,
        amount: float,
        currency: Currency,
        strategy: RoutingStrategy = RoutingStrategy.LOWEST_FEE,
    ) -> RoutingDecision:
        eligible_psps = [PSPProvider.STRIPE, PSPProvider.ADYEN, PSPProvider.CHECKOUT]
        if currency == Currency.EUR:
            eligible_psps.append(PSPProvider.INTERNAL)

        if strategy == RoutingStrategy.LOWEST_FEE:
            ranked = sorted(eligible_psps, key=lambda p: self.psp_fee_bps[p])
        elif strategy == RoutingStrategy.HIGHEST_SUCCESS_RATE:
            ranked = sorted(eligible_psps, key=lambda p: -self.success_rates[p])
        else:
            # Balanced: weight cost and success
            ranked = sorted(
                eligible_psps,
                key=lambda p: (self.psp_fee_bps[p] * 0.4) - (self.success_rates[p] * 100 * 0.6),
            )

        chosen = ranked[0]
        fee = round(amount * (self.psp_fee_bps[chosen] / 10000.0), 2)
        latency = self.expected_latencies[chosen]

        return RoutingDecision(
            payment_id=payment_id,
            selected_psp=chosen,
            alternative_psps=ranked[1:],
            strategy_used=strategy,
            estimated_fee=fee,
            expected_latency_ms=latency,
        )


class PaymentStreamProcessor:
    """Processes real-time payment event streams with idempotent deduplication and state tracking."""

    VALID_TRANSITIONS: ClassVar[dict[PaymentStatus, set[PaymentStatus]]] = {
        PaymentStatus.AUTHORIZED: {PaymentStatus.CAPTURED, PaymentStatus.DECLINED},
        PaymentStatus.CAPTURED: {PaymentStatus.SETTLED, PaymentStatus.REFUNDED, PaymentStatus.DISPUTED},
        PaymentStatus.SETTLED: {PaymentStatus.REFUNDED, PaymentStatus.DISPUTED},
        PaymentStatus.DECLINED: set(),
        PaymentStatus.REFUNDED: set(),
        PaymentStatus.DISPUTED: {PaymentStatus.SETTLED, PaymentStatus.REFUNDED},
    }

    def __init__(self, bus: Bus | None = None) -> None:
        self.bus = bus or InMemoryBus()
        self._processed_keys: set[str] = set()
        self._payment_states: dict[str, PaymentEvent] = {}
        self._event_history: list[PaymentEvent] = []

    def process_event(self, raw_event: PaymentEvent) -> tuple[bool, str, PaymentEvent]:
        # Validate data contracts and GDPR hygiene
        event = PaymentDataQualityValidator.validate_and_sanitize(raw_event)

        # Idempotency verification: deduplicate exact duplicate attempts
        key_hash = hashlib.sha256(f"{event.idempotency_key}:{event.status.value}".encode()).hexdigest()
        if key_hash in self._processed_keys:
            existing = self._payment_states[event.idempotency_key]
            return False, "IDEMPOTENT_DUPLICATE_IGNORED", existing

        # State transition validation
        if event.idempotency_key in self._payment_states:
            current = self._payment_states[event.idempotency_key]
            allowed = self.VALID_TRANSITIONS.get(current.status, set())
            if event.status not in allowed:
                return (
                    False,
                    f"ILLEGAL_STATE_TRANSITION: Cannot transition {current.status.value} to {event.status.value}",
                    current,
                )

        # Record state and publish downstream event
        self._processed_keys.add(key_hash)
        self._payment_states[event.idempotency_key] = event
        self._event_history.append(event)

        self.bus.publish(
            topic="payments.lifecycle.events",
            event_type=f"payment.{event.status.value.lower()}",
            payload={
                "event_id": event.event_id,
                "idempotency_key": event.idempotency_key,
                "merchant_id": event.merchant_id,
                "amount": event.amount,
                "currency": event.currency.value,
                "psp": event.psp.value,
                "status": event.status.value,
                "processing_fee": event.processing_fee,
                "psp_latency_ms": event.psp_latency_ms,
            },
            key=event.idempotency_key,
        )

        return True, "PROCESSED_SUCCESSFULLY", event

    def get_payment_state(self, idempotency_key: str) -> PaymentEvent | None:
        return self._payment_states.get(idempotency_key)

    @property
    def total_events(self) -> int:
        return len(self._event_history)


class PaymentReconciliationEngine:
    """Reconciles internal transaction ledgers against processor settlement batches."""

    @classmethod
    def reconcile(
        cls,
        ledger_events: list[PaymentEvent],
        settlement_records: list[SettlementRecord],
        fee_tolerance: float = 0.05,
    ) -> ReconciliationReport:
        ledger_by_id = {e.idempotency_key: e for e in ledger_events if e.status in (PaymentStatus.CAPTURED, PaymentStatus.SETTLED)}
        settlement_by_id = {s.idempotency_key: s for s in settlement_records}

        discrepancies: list[DiscrepancyItem] = []
        matched_count = 0
        total_discrepancy_amount = 0.0

        all_keys = set(ledger_by_id.keys()).union(settlement_by_id.keys())

        for key in all_keys:
            in_ledger = ledger_by_id.get(key)
            in_settle = settlement_by_id.get(key)

            if in_ledger and not in_settle:
                diff = in_ledger.amount
                total_discrepancy_amount += diff
                discrepancies.append(
                    DiscrepancyItem(
                        payment_id=in_ledger.event_id,
                        status=ReconciliationStatus.UNMATCHED_PROCESSOR,
                        ledger_amount=in_ledger.amount,
                        processor_amount=None,
                        difference=diff,
                        reason="Transaction captured in ledger but missing from PSP settlement batch.",
                    )
                )
            elif in_settle and not in_ledger:
                diff = in_settle.gross_amount
                total_discrepancy_amount += diff
                discrepancies.append(
                    DiscrepancyItem(
                        payment_id=in_settle.payment_id,
                        status=ReconciliationStatus.UNMATCHED_LEDGER,
                        ledger_amount=None,
                        processor_amount=in_settle.gross_amount,
                        difference=diff,
                        reason="Settlement found on processor but not in captured ledger.",
                    )
                )
            elif in_ledger and in_settle:
                amount_diff = abs(in_ledger.amount - in_settle.gross_amount)
                if amount_diff > 0.001:
                    total_discrepancy_amount += amount_diff
                    discrepancies.append(
                        DiscrepancyItem(
                            payment_id=in_ledger.event_id,
                            status=ReconciliationStatus.AMOUNT_MISMATCH,
                            ledger_amount=in_ledger.amount,
                            processor_amount=in_settle.gross_amount,
                            difference=amount_diff,
                            reason=f"Gross amount mismatch: ledger={in_ledger.amount}, processor={in_settle.gross_amount}",
                        )
                    )
                else:
                    # Check fee discrepancy if fee was recorded
                    fee_diff = abs(in_ledger.processing_fee - in_settle.fee)
                    if in_ledger.processing_fee > 0 and fee_diff > fee_tolerance:
                        total_discrepancy_amount += fee_diff
                        discrepancies.append(
                            DiscrepancyItem(
                                payment_id=in_ledger.event_id,
                                status=ReconciliationStatus.FEE_DISCREPANCY,
                                ledger_amount=in_ledger.processing_fee,
                                processor_amount=in_settle.fee,
                                difference=fee_diff,
                                reason=f"Processor fee exceeds expected tolerance: diff={fee_diff:.2f}",
                            )
                        )
                    else:
                        matched_count += 1

        return ReconciliationReport(
            total_ledger_records=len(ledger_by_id),
            total_processor_records=len(settlement_by_id),
            matched_count=matched_count,
            discrepancy_count=len(discrepancies),
            total_discrepancy_amount=round(total_discrepancy_amount, 2),
            discrepancies=discrepancies,
        )


class PaymentWarehouseExporter:
    """Prepares structured, partitioned analytical records for Snowflake and BigQuery."""

    @classmethod
    def aggregate_metrics(cls, events: list[PaymentEvent]) -> AnalyticalMetrics:
        if not events:
            return AnalyticalMetrics(
                total_volume=0.0,
                transaction_count=0,
                conversion_rate=0.0,
                total_fees=0.0,
                p95_latency_ms=0.0,
                volume_by_psp={},
                success_rate_by_psp={},
                estimated_routing_savings=0.0,
            )

        total_vol = sum(e.amount for e in events)
        successful = [e for e in events if e.status in (PaymentStatus.CAPTURED, PaymentStatus.SETTLED)]
        conv_rate = round(len(successful) / len(events), 4)
        total_fees = sum(e.processing_fee for e in events)

        # Latencies
        latencies = sorted(e.psp_latency_ms for e in events)
        p95_idx = int(len(latencies) * 0.95)
        p95 = latencies[min(p95_idx, len(latencies) - 1)]

        # Volume & success by PSP
        vol_by_psp: dict[str, float] = {}
        success_by_psp: dict[str, list[bool]] = {}

        for e in events:
            psp_name = e.psp.value
            vol_by_psp[psp_name] = round(vol_by_psp.get(psp_name, 0.0) + e.amount, 2)
            if psp_name not in success_by_psp:
                success_by_psp[psp_name] = []
            success_by_psp[psp_name].append(e.status in (PaymentStatus.CAPTURED, PaymentStatus.SETTLED))

        rates = {
            psp: round(sum(flags) / len(flags), 4)
            for psp, flags in success_by_psp.items()
        }

        # Routing savings: difference compared to default unoptimized benchmark (2.4% baseline fee)
        baseline_fee = total_vol * 0.024
        estimated_savings = max(0.0, round(baseline_fee - total_fees, 2))

        return AnalyticalMetrics(
            total_volume=round(total_vol, 2),
            transaction_count=len(events),
            conversion_rate=conv_rate,
            total_fees=round(total_fees, 2),
            p95_latency_ms=round(p95, 2),
            volume_by_psp=vol_by_psp,
            success_rate_by_psp=rates,
            estimated_routing_savings=estimated_savings,
        )

    @classmethod
    def generate_snowflake_ddl(cls) -> str:
        """Returns production-grade Snowflake DDL for staging and analytics tables."""
        return """
-- Snowflake DDL for Payment Lifecycle Analytics
CREATE OR REPLACE TABLE raw_payment_events (
    event_id VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    merchant_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    amount NUMBER(18, 4) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    psp VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    processing_fee NUMBER(12, 4) DEFAULT 0,
    psp_latency_ms FLOAT,
    occurred_at TIMESTAMP_NTZ NOT NULL,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    masked_pan VARCHAR(32),
    metadata VARIANT,
    PRIMARY KEY (event_id)
)
CLUSTER BY (occurred_at, merchant_id, psp);

CREATE OR REPLACE TABLE daily_payment_reconciliation_summary (
    reconciliation_date DATE NOT NULL,
    merchant_id VARCHAR(64) NOT NULL,
    matched_count NUMBER(12, 0) NOT NULL,
    discrepancy_count NUMBER(12, 0) NOT NULL,
    total_volume NUMBER(18, 4) NOT NULL,
    total_fees NUMBER(18, 4) NOT NULL,
    is_balanced BOOLEAN NOT NULL,
    PRIMARY KEY (reconciliation_date, merchant_id)
);
""".strip()
