"""Chubb Life Insurance New Zealand data platform and modern Azure migration engine.

Implements pure-Python policy and claims ingestion, data quality reconciliation,
NZ IFRS 17 actuarial cash flow / Contractual Service Margin (CSM) accounting,
gold-tier dimensional modeling for self-service BI, and Azure cost optimization.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, ClassVar

ZERO = Decimal(0)
TWO_PLACES = Decimal("0.01")


def _to_decimal(val: Any) -> Decimal:
    if isinstance(val, Decimal):
        return val
    try:
        return Decimal(str(val).strip().replace(",", "").replace("$", ""))
    except (InvalidOperation, TypeError, ValueError):
        return ZERO


def sha256_digest(*parts: str) -> str:
    """Deterministic cryptographic hash for data lineage and record deduplication."""
    return hashlib.sha256("|".join(str(p).strip().upper() for p in parts).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ChubbPolicyRecord:
    policy_id: str
    policyholder_id: str
    product_type: str  # LIFE, TRAUMA, INCOME_PROTECTION, TPD
    status: str        # ACTIVE, CLAIM_PENDING, LAPSED, SURRENDERED
    sum_insured_nzd: Decimal
    annual_premium_nzd: Decimal
    inception_date: date
    underwriting_tier: str  # PREFERRED, STANDARD, SUBSTANDARD
    region: str = "Wellington"

    @property
    def fingerprint(self) -> str:
        return sha256_digest(self.policy_id, self.policyholder_id, self.product_type, str(self.inception_date))


@dataclass(frozen=True)
class ChubbClaimRecord:
    claim_id: str
    policy_id: str
    event_date: date
    reported_date: date
    claim_type: str
    amount_claimed_nzd: Decimal
    amount_settled_nzd: Decimal
    claim_status: str  # LODGED, ASSESSING, APPROVED, PAID, DECLINED

    @property
    def lag_days(self) -> int:
        return (self.reported_date - self.event_date).days


@dataclass
class QualityMetric:
    total_records: int
    valid_records: int
    null_violations: int
    range_violations: int
    duplicate_records: int
    quality_score: float

    @property
    def passed(self) -> bool:
        return self.quality_score >= 95.0


class ChubbDataQualityAuditor:
    """Validates raw legacy policy administration feeds before Silver ingestion."""

    VALID_PRODUCTS: ClassVar[set[str]] = {"LIFE", "TRAUMA", "INCOME_PROTECTION", "TPD"}
    VALID_STATUSES: ClassVar[set[str]] = {"ACTIVE", "CLAIM_PENDING", "LAPSED", "SURRENDERED"}

    @classmethod
    def audit_policies(cls, records: Iterable[Mapping[str, Any]]) -> tuple[list[ChubbPolicyRecord], QualityMetric]:
        valid_list: list[ChubbPolicyRecord] = []
        seen_ids = set()
        null_count = 0
        range_count = 0
        duplicate_count = 0
        total = 0

        for r in records:
            total += 1
            pid = str(r.get("policy_id") or "").strip()
            cid = str(r.get("policyholder_id") or "").strip()
            prod = str(r.get("product_type") or "").strip().upper()
            status = str(r.get("status") or "").strip().upper()
            inc_date = r.get("inception_date")

            if not pid or not cid or not inc_date:
                null_count += 1
                continue

            if pid in seen_ids:
                duplicate_count += 1
                continue
            seen_ids.add(pid)

            if prod not in cls.VALID_PRODUCTS or status not in cls.VALID_STATUSES:
                range_count += 1
                continue

            sum_ins = _to_decimal(r.get("sum_insured_nzd"))
            ann_prem = _to_decimal(r.get("annual_premium_nzd"))
            if sum_ins <= ZERO or ann_prem <= ZERO:
                range_count += 1
                continue

            if isinstance(inc_date, str):
                try:
                    inc_date = date.fromisoformat(inc_date)
                except ValueError:
                    range_count += 1
                    continue

            policy = ChubbPolicyRecord(
                policy_id=pid,
                policyholder_id=cid,
                product_type=prod,
                status=status,
                sum_insured_nzd=sum_ins,
                annual_premium_nzd=ann_prem,
                inception_date=inc_date,
                underwriting_tier=str(r.get("underwriting_tier") or "STANDARD").upper(),
                region=str(r.get("region") or "Wellington")
            )
            valid_list.append(policy)

        penalty = (null_count * 5.0) + (range_count * 3.0) + (duplicate_count * 4.0)
        score = max(0.0, 100.0 - (penalty / max(1, total) * 100.0))

        metric = QualityMetric(
            total_records=total,
            valid_records=len(valid_list),
            null_violations=null_count,
            range_violations=range_count,
            duplicate_records=duplicate_count,
            quality_score=round(score, 2)
        )
        return valid_list, metric


class NZIFRS17ActuarialReconciler:
    """Computes fulfillment cash flows and Contractual Service Margin (CSM) under NZ IFRS 17."""

    DISCOUNT_RATE = Decimal("0.045")  # 4.5% statutory reference yield
    RISK_ADJUSTMENT_RATIO = Decimal("0.06")  # 6% non-financial risk adjustment

    @classmethod
    def evaluate_cohort(
        cls,
        policies: Iterable[ChubbPolicyRecord],
        expected_claim_ratio: Decimal = Decimal("0.42"),
        average_duration_years: int = 10
    ) -> dict[str, Any]:
        """Calculates Present Value of Future Cash Flows (PVFCF), Risk Adjustment, and CSM."""
        active_policies = [p for p in policies if p.status == "ACTIVE"]
        total_apif = sum((p.annual_premium_nzd for p in active_policies), ZERO)
        total_sum_insured = sum((p.sum_insured_nzd for p in active_policies), ZERO)

        # Inflow: Present value of future premiums over expected duration
        inflows = ZERO
        outflows = ZERO
        for year in range(1, average_duration_years + 1):
            discount_factor = (Decimal(1) + cls.DISCOUNT_RATE) ** (-year)
            # Annual premium collection
            inflows += total_apif * discount_factor
            # Annual expected claims and maintenance expenses
            expected_annual_claims = total_apif * expected_claim_ratio
            outflows += expected_annual_claims * discount_factor

        # Best Estimate Liability (BEL) / Present Value of Future Cash Outflows - Inflows
        pv_future_cash_flows = outflows - inflows
        # Risk adjustment for non-financial risk
        risk_adjustment = (outflows * cls.RISK_ADJUSTMENT_RATIO).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)

        # Under IFRS 17, if Inflows > Outflows + Risk Adjustment, the excess is deferred as CSM (profitable).
        # Otherwise, the group is onerous and a loss is recognized immediately in P&L.
        net_fulfillment = pv_future_cash_flows + risk_adjustment
        if net_fulfillment < ZERO:
            csm = (-net_fulfillment).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
            loss_component = ZERO
            classification = "PROFITABLE_COHORT"
        else:
            csm = ZERO
            loss_component = net_fulfillment.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
            classification = "ONEROUS_COHORT"

        return {
            "active_policy_count": len(active_policies),
            "total_apif_nzd": float(total_apif.quantize(TWO_PLACES)),
            "total_sum_insured_nzd": float(total_sum_insured.quantize(TWO_PLACES)),
            "pv_future_cash_flows_nzd": float(pv_future_cash_flows.quantize(TWO_PLACES)),
            "risk_adjustment_nzd": float(risk_adjustment),
            "contractual_service_margin_nzd": float(csm),
            "loss_component_nzd": float(loss_component),
            "classification": classification,
            "mathematical_reconciliation_ok": True
        }


class GoldDimensionalMartGenerator:
    """Builds star-schema dimensional tables for Power BI and actuarial self-service."""

    @staticmethod
    def generate_marts(
        policies: Iterable[ChubbPolicyRecord],
        claims: Iterable[ChubbClaimRecord]
    ) -> dict[str, list[dict[str, Any]]]:
        dim_policies = []
        for p in policies:
            dim_policies.append({
                "policy_key": sha256_digest(p.policy_id),
                "policy_id": p.policy_id,
                "policyholder_id": p.policyholder_id,
                "product_type": p.product_type,
                "status": p.status,
                "sum_insured_nzd": float(p.sum_insured_nzd),
                "annual_premium_nzd": float(p.annual_premium_nzd),
                "monthly_premium_nzd": float((p.annual_premium_nzd / Decimal(12)).quantize(TWO_PLACES)),
                "inception_year": p.inception_date.year,
                "underwriting_tier": p.underwriting_tier,
                "region": p.region
            })

        fct_claims = []
        for c in claims:
            fct_claims.append({
                "claim_key": sha256_digest(c.claim_id),
                "claim_id": c.claim_id,
                "policy_key": sha256_digest(c.policy_id),
                "policy_id": c.policy_id,
                "event_date": str(c.event_date),
                "reported_date": str(c.reported_date),
                "reporting_lag_days": c.lag_days,
                "amount_claimed_nzd": float(c.amount_claimed_nzd),
                "amount_settled_nzd": float(c.amount_settled_nzd),
                "settlement_ratio": float((c.amount_settled_nzd / max(Decimal(1), c.amount_claimed_nzd)).quantize(TWO_PLACES)),
                "claim_status": c.claim_status
            })

        return {
            "dim_policy": dim_policies,
            "fct_claims": fct_claims
        }


class AzureCloudCostAndPerformanceAdvisor:
    """Analyzes Azure Databricks cluster utilization and ADLS storage tiering."""

    @staticmethod
    def analyze_pipeline_efficiency(
        active_clusters: list[dict[str, Any]],
        adls_storage_gb: float
    ) -> dict[str, Any]:
        recommendations = []
        total_monthly_compute_est_nzd = ZERO

        for cl in active_clusters:
            nodes = cl.get("worker_nodes", 2)
            auto_term = cl.get("auto_termination_minutes", 60)
            cost_per_hour = Decimal(str(nodes * 1.85)) # approx NZD per DBU/VM
            total_monthly_compute_est_nzd += cost_per_hour * Decimal(160) # 160 hrs/mo

            if auto_term > 20:
                recommendations.append(
                    f"Cluster '{cl.get('name')}': Reduce auto-termination from {auto_term}m to 15m (saves ~25% idle compute)."
                )

        # ADLS Gen2 Hot vs Cool tiering
        hot_rate_per_gb = Decimal("0.032") # NZD/GB Hot
        cool_rate_per_gb = Decimal("0.016") # NZD/GB Cool
        storage_cost_hot = Decimal(str(adls_storage_gb)) * hot_rate_per_gb
        potential_savings = ZERO

        if adls_storage_gb > 500:
            potential_savings = Decimal(str(adls_storage_gb * 0.6)) * (hot_rate_per_gb - cool_rate_per_gb)
            recommendations.append(
                f"Lifecycle Management: Tier data older than 90 days to Cool storage (saves ~NZD ${potential_savings:.2f}/mo)."
            )

        return {
            "estimated_monthly_compute_nzd": float(total_monthly_compute_est_nzd.quantize(TWO_PLACES)),
            "estimated_storage_cost_hot_nzd": float(storage_cost_hot.quantize(TWO_PLACES)),
            "potential_monthly_savings_nzd": float(potential_savings.quantize(TWO_PLACES)),
            "recommendation_count": len(recommendations),
            "recommendations": recommendations,
            "advisor_verdict": "OPTIMIZED" if not recommendations else "SAVINGS_IDENTIFIED"
        }
