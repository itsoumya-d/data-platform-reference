"""Unit tests for Woolworths Group New Zealand Retail Data Science module."""

from datetime import date, timedelta
import pytest

from dataplat.woolworths_retail_ds import (
    CustomerProfile,
    EverydayRewardsSegmentation,
    MultiCloudRetailETLPipeline,
    ResponsibleAIGovernance,
    RetailDemandForecastingEngine,
    StoreProductSales,
)


@pytest.fixture
def sample_sales_history():
    """Generates 14 days of realistic store sales history."""
    base_date = date(2026, 11, 1)
    history = []
    # Stable daily sales around 50 units with minor variance
    daily_units = [48, 52, 50, 49, 53, 51, 47, 50, 52, 48, 55, 49, 51, 50]
    for i, u in enumerate(daily_units):
        history.append(
            StoreProductSales(
                store_id="STORE_1042_PONSONBY",
                product_sku="SKU_ORGANIC_MILK_2L",
                sales_date=base_date + timedelta(days=i),
                units_sold=float(u),
                unit_price_nzd=4.99,
                is_promotional=False,
                inventory_on_hand=60.0,
            )
        )
    return history


def test_baseline_demand_calculation(sample_sales_history):
    engine = RetailDemandForecastingEngine()
    mean, std = engine.calculate_baseline_demand(sample_sales_history)
    assert 49.0 <= mean <= 52.0
    assert 1.0 <= std <= 3.0


def test_forecast_demand_standard(sample_sales_history):
    engine = RetailDemandForecastingEngine(service_level=0.95, shelf_life_days=7)
    res = engine.forecast_demand(
        store_id="STORE_1042_PONSONBY",
        product_sku="SKU_ORGANIC_MILK_2L",
        target_date=date(2026, 11, 15),
        history=sample_sales_history,
        is_promo=False,
        inventory_on_hand=20.0,
    )
    assert res.store_id == "STORE_1042_PONSONBY"
    assert res.product_sku == "SKU_ORGANIC_MILK_2L"
    assert res.predicted_units > 45.0
    assert res.confidence_interval_lower < res.predicted_units < res.confidence_interval_upper
    # Recommended order = target stock - 20 inventory
    assert res.recommended_replenishment_units > 25.0
    # Current inventory 20 vs 7 days capacity (~350) -> low shrinkage risk
    assert res.shrinkage_risk_score == 0.0


def test_forecast_demand_promotional_lift(sample_sales_history):
    engine = RetailDemandForecastingEngine()
    res_normal = engine.forecast_demand(
        store_id="STORE_1042_PONSONBY",
        product_sku="SKU_ORGANIC_MILK_2L",
        target_date=date(2026, 11, 15),
        history=sample_sales_history,
        is_promo=False,
    )
    res_promo = engine.forecast_demand(
        store_id="STORE_1042_PONSONBY",
        product_sku="SKU_ORGANIC_MILK_2L",
        target_date=date(2026, 11, 15),
        history=sample_sales_history,
        is_promo=True,
        promo_multiplier=1.40,
    )
    assert res_promo.predicted_units > res_normal.predicted_units
    assert pytest.approx(res_promo.predicted_units, 0.1) == res_normal.predicted_units * 1.40


def test_shrinkage_waste_risk_detection(sample_sales_history):
    engine = RetailDemandForecastingEngine(shelf_life_days=3)
    # Excess inventory 500 units when daily sales is ~50 and shelf life is 3 days (150 capacity)
    res = engine.forecast_demand(
        store_id="STORE_1042_PONSONBY",
        product_sku="SKU_ORGANIC_MILK_2L",
        target_date=date(2026, 11, 15),
        history=sample_sales_history,
        inventory_on_hand=500.0,
    )
    assert res.shrinkage_risk_score > 0.5
    assert res.recommended_replenishment_units == 0.0


def test_everyday_rewards_segmentation_platinum():
    segmenter = EverydayRewardsSegmentation()
    profile = CustomerProfile(
        customer_id="CUST_889211",
        recency_days=2,
        frequency_annual=48,
        monetary_spend_nzd=3200.0,
        digital_app_user=True,
    )
    res = segmenter.segment_customer(profile)
    assert res["assigned_tier"] == "Platinum Champion"
    assert "Double Rewards" in res["recommended_offer"]
    assert res["digital_engagement_priority"] == "Push Notification"


def test_everyday_rewards_segmentation_bronze():
    segmenter = EverydayRewardsSegmentation()
    profile = CustomerProfile(
        customer_id="CUST_110294",
        recency_days=85,
        frequency_annual=2,
        monetary_spend_nzd=120.0,
        digital_app_user=False,
    )
    res = segmenter.segment_customer(profile)
    assert res["assigned_tier"] == "Bronze Saver"
    assert "Welcome back" in res["recommended_offer"]
    assert res["digital_engagement_priority"] == "Email Digest"


def test_multicloud_etl_pipeline_query_generation():
    gcp_pipeline = MultiCloudRetailETLPipeline(target_cloud="GCP")
    gcp_sql = gcp_pipeline.generate_extraction_query(
        store_id="STORE_1042",
        start_date=date(2026, 11, 1),
        end_date=date(2026, 11, 7),
    )
    assert "wwnz-analytics-prod.retail_pos.store_daily_sales" in gcp_sql
    assert "WHERE store_id = 'STORE_1042'" in gcp_sql

    azure_pipeline = MultiCloudRetailETLPipeline(target_cloud="Azure")
    azure_sql = azure_pipeline.generate_extraction_query(
        store_id="STORE_1042",
        start_date=date(2026, 11, 1),
        end_date=date(2026, 11, 7),
    )
    assert "wwnz_azure_lakehouse.retail_pos.store_daily_sales" in azure_sql


def test_multicloud_etl_transform_and_validate():
    pipeline = MultiCloudRetailETLPipeline()
    raw = [
        {
            "store_id": "STORE_1042",
            "product_sku": "SKU_BREAD_WHOLEMEAL",
            "sales_date": "2026-11-04",
            "units_sold": "35.5",
            "unit_price_nzd": "3.50",
            "is_promotional": 1,
            "inventory_on_hand": "80",
        }
    ]
    validated = pipeline.transform_and_validate(raw)
    assert len(validated) == 1
    v = validated[0]
    assert v.store_id == "STORE_1042"
    assert v.units_sold == 35.5
    assert v.sales_date == date(2026, 11, 4)
    assert v.is_promotional is True


def test_responsible_ai_governance_privacy():
    masked = ResponsibleAIGovernance.mask_customer_identifier("CUST_NZ_998822")
    assert masked.startswith("ANON_")
    assert len(masked) == 17
    # Idempotent
    assert masked == ResponsibleAIGovernance.mask_customer_identifier("CUST_NZ_998822")


def test_responsible_ai_pricing_fairness():
    # Compliant discount: $100 -> $70 (30% discount <= 40% cap)
    audit = ResponsibleAIGovernance.audit_pricing_fairness(base_price=100.0, discounted_price=70.0)
    assert audit["fair"] is True
    assert audit["discount_ratio"] == 0.30

    # Non-compliant predatory discount: $100 -> $40 (60% discount > 40% cap)
    audit_excessive = ResponsibleAIGovernance.audit_pricing_fairness(base_price=100.0, discounted_price=40.0)
    assert audit_excessive["fair"] is False
    assert audit_excessive["adjusted_price"] == 60.0
