"""Unit test suite for Chubb Life Insurance New Zealand modern data platform components."""

from datetime import date
from decimal import Decimal

from dataplat.chubb_life_data_platform import (
    AzureCloudCostAndPerformanceAdvisor,
    ChubbClaimRecord,
    ChubbDataQualityAuditor,
    ChubbPolicyRecord,
    GoldDimensionalMartGenerator,
    NZIFRS17ActuarialReconciler,
    sha256_digest,
)


def test_chubb_policy_record_hashing_and_properties():
    policy = ChubbPolicyRecord(
        policy_id="POL-NZ-9001",
        policyholder_id="CUST-7701",
        product_type="LIFE",
        status="ACTIVE",
        sum_insured_nzd=Decimal("750000.00"),
        annual_premium_nzd=Decimal("1250.00"),
        inception_date=date(2023, 5, 15),
        underwriting_tier="PREFERRED",
        region="Wellington"
    )
    assert policy.policy_id == "POL-NZ-9001"
    assert policy.sum_insured_nzd == Decimal("750000.00")
    assert policy.fingerprint == sha256_digest("POL-NZ-9001", "CUST-7701", "LIFE", "2023-05-15")


def test_chubb_claims_lag_days():
    claim = ChubbClaimRecord(
        claim_id="CLM-NZ-4412",
        policy_id="POL-NZ-9001",
        event_date=date(2026, 8, 1),
        reported_date=date(2026, 8, 11),
        claim_type="TRAUMA",
        amount_claimed_nzd=Decimal("150000.00"),
        amount_settled_nzd=Decimal("150000.00"),
        claim_status="PAID"
    )
    assert claim.lag_days == 10
    assert claim.amount_settled_nzd == Decimal("150000.00")


def test_data_quality_auditor_clean_records():
    raw_data = [
        {
            "policy_id": "P01",
            "policyholder_id": "C01",
            "product_type": "LIFE",
            "status": "ACTIVE",
            "sum_insured_nzd": "500000",
            "annual_premium_nzd": "1200",
            "inception_date": "2022-01-10",
            "underwriting_tier": "PREFERRED"
        },
        {
            "policy_id": "P02",
            "policyholder_id": "C02",
            "product_type": "INCOME_PROTECTION",
            "status": "ACTIVE",
            "sum_insured_nzd": "350000",
            "annual_premium_nzd": "950",
            "inception_date": "2023-06-20",
            "underwriting_tier": "STANDARD"
        }
    ]
    policies, metric = ChubbDataQualityAuditor.audit_policies(raw_data)
    assert len(policies) == 2
    assert metric.total_records == 2
    assert metric.valid_records == 2
    assert metric.null_violations == 0
    assert metric.range_violations == 0
    assert metric.duplicate_records == 0
    assert metric.quality_score == 100.0
    assert metric.passed is True


def test_data_quality_auditor_catches_violations():
    dirty_data = [
        {"policy_id": "", "policyholder_id": "C01"}, # null violation
        {"policy_id": "P01", "policyholder_id": "C01", "product_type": "LIFE", "status": "ACTIVE", "sum_insured_nzd": "500000", "annual_premium_nzd": "1200", "inception_date": "2022-01-10"},
        {"policy_id": "P01", "policyholder_id": "C01", "product_type": "LIFE", "status": "ACTIVE", "sum_insured_nzd": "500000", "annual_premium_nzd": "1200", "inception_date": "2022-01-10"}, # duplicate
        {"policy_id": "P02", "policyholder_id": "C02", "product_type": "INVALID_PROD", "status": "ACTIVE", "sum_insured_nzd": "-100", "annual_premium_nzd": "100", "inception_date": "2022-01-10"}, # range violation
    ]
    policies, metric = ChubbDataQualityAuditor.audit_policies(dirty_data)
    assert len(policies) == 1
    assert metric.total_records == 4
    assert metric.null_violations == 1
    assert metric.duplicate_records == 1
    assert metric.range_violations == 1
    assert metric.quality_score < 90.0
    assert metric.passed is False


def test_nz_ifrs17_profitable_cohort_evaluation():
    policies = [
        ChubbPolicyRecord(
            policy_id=f"POL-{i}",
            policyholder_id=f"CUST-{i}",
            product_type="LIFE",
            status="ACTIVE",
            sum_insured_nzd=Decimal(500000),
            annual_premium_nzd=Decimal(1500),
            inception_date=date(2024, 1, 1),
            underwriting_tier="STANDARD"
        )
        for i in range(10)
    ]
    # Total APIF = $15,000/yr, low expected claims ratio = 35% -> highly profitable cohort
    cohort = NZIFRS17ActuarialReconciler.evaluate_cohort(
        policies,
        expected_claim_ratio=Decimal("0.35"),
        average_duration_years=10
    )
    assert cohort["active_policy_count"] == 10
    assert cohort["total_apif_nzd"] == 15000.0
    assert cohort["classification"] == "PROFITABLE_COHORT"
    assert cohort["contractual_service_margin_nzd"] > 0
    assert cohort["loss_component_nzd"] == 0.0
    assert cohort["mathematical_reconciliation_ok"] is True


def test_nz_ifrs17_onerous_cohort_evaluation():
    policies = [
        ChubbPolicyRecord(
            policy_id="POL-ONEROUS-1",
            policyholder_id="CUST-HIGH-RISK",
            product_type="TRAUMA",
            status="ACTIVE",
            sum_insured_nzd=Decimal(1000000),
            annual_premium_nzd=Decimal(800), # underpriced premium
            inception_date=date(2024, 1, 1),
            underwriting_tier="SUBSTANDARD"
        )
    ]
    # Expected claim ratio 150% (claims far exceed premium)
    cohort = NZIFRS17ActuarialReconciler.evaluate_cohort(
        policies,
        expected_claim_ratio=Decimal("1.50"),
        average_duration_years=5
    )
    assert cohort["classification"] == "ONEROUS_COHORT"
    assert cohort["contractual_service_margin_nzd"] == 0.0
    assert cohort["loss_component_nzd"] > 0
    assert cohort["mathematical_reconciliation_ok"] is True


def test_gold_dimensional_mart_generation():
    policies = [
        ChubbPolicyRecord(
            policy_id="POL-G01",
            policyholder_id="CUST-G01",
            product_type="LIFE",
            status="ACTIVE",
            sum_insured_nzd=Decimal(600000),
            annual_premium_nzd=Decimal(1800),
            inception_date=date(2023, 3, 1),
            underwriting_tier="PREFERRED",
            region="Wellington"
        )
    ]
    claims = [
        ChubbClaimRecord(
            claim_id="CLM-G01",
            policy_id="POL-G01",
            event_date=date(2026, 1, 15),
            reported_date=date(2026, 1, 25),
            claim_type="LIFE",
            amount_claimed_nzd=Decimal(600000),
            amount_settled_nzd=Decimal(600000),
            claim_status="PAID"
        )
    ]
    marts = GoldDimensionalMartGenerator.generate_marts(policies, claims)
    assert "dim_policy" in marts
    assert "fct_claims" in marts

    p_mart = marts["dim_policy"][0]
    assert p_mart["policy_id"] == "POL-G01"
    assert p_mart["monthly_premium_nzd"] == 150.0 # 1800 / 12
    assert p_mart["inception_year"] == 2023

    c_mart = marts["fct_claims"][0]
    assert c_mart["claim_id"] == "CLM-G01"
    assert c_mart["reporting_lag_days"] == 10
    assert c_mart["settlement_ratio"] == 1.0


def test_azure_cloud_cost_and_performance_advisor():
    clusters = [
        {"name": "actuarial-batch-prod", "worker_nodes": 8, "auto_termination_minutes": 60},
        {"name": "bi-interactive", "worker_nodes": 4, "auto_termination_minutes": 15}
    ]
    report = AzureCloudCostAndPerformanceAdvisor.analyze_pipeline_efficiency(
        active_clusters=clusters,
        adls_storage_gb=2500.0 # 2.5 TB
    )
    assert report["advisor_verdict"] == "SAVINGS_IDENTIFIED"
    assert report["recommendation_count"] >= 2
    assert report["potential_monthly_savings_nzd"] > 0
    # Proves detection of oversized auto-termination timeout
    assert any("Reduce auto-termination" in r for r in report["recommendations"])
    # Proves storage tiering savings
    assert any("Tier data older than 90 days to Cool storage" in r for r in report["recommendations"])
