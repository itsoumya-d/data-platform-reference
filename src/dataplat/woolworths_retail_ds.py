"""Woolworths Group New Zealand (wiqNZ) - Retail Data Science & Demand Forecasting Engine.

Implements production-grade machine learning algorithms, customer segmentation,
multi-cloud ETL workflows (GCP BigQuery & Azure), and Responsible AI governance.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class StoreProductSales:
    """Historical point-of-sale transactional record."""
    store_id: str
    product_sku: str
    sales_date: date
    units_sold: float
    unit_price_nzd: float
    is_promotional: bool = False
    is_holiday: bool = False
    inventory_on_hand: float = 0.0


@dataclass
class DemandForecastResult:
    """Predicted demand and inventory replenishment recommendation."""
    store_id: str
    product_sku: str
    forecast_date: date
    predicted_units: float
    confidence_interval_lower: float
    confidence_interval_upper: float
    recommended_replenishment_units: float
    shrinkage_risk_score: float  # 0.0 to 1.0 (waste risk)


class RetailDemandForecastingEngine:
    """Production statistical and machine learning demand forecasting engine."""

    def __init__(self, service_level: float = 0.95, shelf_life_days: int = 7):
        self.service_level = service_level
        self.shelf_life_days = shelf_life_days
        # Normal distribution z-score for service level
        self._z_score = 1.645 if service_level >= 0.95 else 1.28

    def calculate_baseline_demand(self, history: List[StoreProductSales]) -> Tuple[float, float]:
        """Calculates rolling mean and standard deviation from POS history."""
        if not history:
            return 0.0, 0.0
        units = [s.units_sold for s in history]
        mean = sum(units) / len(units)
        variance = sum((u - mean) ** 2 for u in units) / (len(units) if len(units) == 1 else len(units) - 1)
        std_dev = math.sqrt(variance)
        return mean, std_dev

    def forecast_demand(
        self,
        store_id: str,
        product_sku: str,
        target_date: date,
        history: List[StoreProductSales],
        is_promo: bool = False,
        promo_multiplier: float = 1.35,
        inventory_on_hand: float = 0.0,
    ) -> DemandForecastResult:
        """Forecasts store-level demand applying promotional lift and safety stock."""
        mean, std_dev = self.calculate_baseline_demand(history)

        # Apply promotional multiplier
        pred_units = mean * (promo_multiplier if is_promo else 1.0)
        margin_of_error = self._z_score * std_dev

        lower = max(0.0, pred_units - margin_of_error)
        upper = pred_units + margin_of_error

        # Recommended replenishment: Target = Predicted + Safety Stock - Current Inventory
        safety_stock = self._z_score * std_dev
        target_stock = pred_units + safety_stock
        recommended_order = max(0.0, target_stock - inventory_on_hand)

        # Shrinkage (spoilage/waste) risk score based on inventory vs shelf life
        excess_stock = max(0.0, inventory_on_hand - (mean * self.shelf_life_days))
        shrinkage_risk = min(1.0, excess_stock / (mean * self.shelf_life_days + 1e-5))

        return DemandForecastResult(
            store_id=store_id,
            product_sku=product_sku,
            forecast_date=target_date,
            predicted_units=round(pred_units, 2),
            confidence_interval_lower=round(lower, 2),
            confidence_interval_upper=round(upper, 2),
            recommended_replenishment_units=round(recommended_order, 2),
            shrinkage_risk_score=round(shrinkage_risk, 3),
        )


@dataclass
class CustomerProfile:
    """Everyday Rewards customer transaction profile."""
    customer_id: str
    recency_days: int
    frequency_annual: int
    monetary_spend_nzd: float
    fresh_food_share: float = 0.0
    digital_app_user: bool = False


class EverydayRewardsSegmentation:
    """Customer segmentation and personalized propensity engine."""

    TIERS = ["Bronze Saver", "Silver Value", "Gold Enthusiast", "Platinum Champion"]

    def segment_customer(self, profile: CustomerProfile) -> Dict[str, Any]:
        """Calculates RFM composite score and assigns loyalty tier and offer propensity."""
        # Normalize RFM components (0 to 10 scale)
        r_score = max(0.0, min(10.0, 10.0 - (profile.recency_days / 10.0)))
        f_score = min(10.0, profile.frequency_annual / 12.0 * 2.5)
        m_score = min(10.0, profile.monetary_spend_nzd / 2500.0 * 10.0)

        composite_rfm = (r_score * 0.3) + (f_score * 0.3) + (m_score * 0.4)

        if composite_rfm >= 7.5:
            tier = "Platinum Champion"
            recommended_offer = "15% off Fresh & Organic Produce + Double Rewards Points"
        elif composite_rfm >= 5.0:
            tier = "Gold Enthusiast"
            recommended_offer = "$10 voucher on $80 grocery basket"
        elif composite_rfm >= 2.5:
            tier = "Silver Value"
            recommended_offer = "Personalized discounts on frequently purchased staples"
        else:
            tier = "Bronze Saver"
            recommended_offer = "Welcome back bonus: 500 Everyday Rewards points on next shop"

        return {
            "customer_id": profile.customer_id,
            "composite_rfm_score": round(composite_rfm, 2),
            "assigned_tier": tier,
            "recommended_offer": recommended_offer,
            "digital_engagement_priority": "Push Notification" if profile.digital_app_user else "Email Digest",
        }


class MultiCloudRetailETLPipeline:
    """Multi-cloud retail ETL pipeline supporting Google Cloud Platform and Microsoft Azure."""

    def __init__(self, target_cloud: str = "GCP"):
        self.target_cloud = target_cloud.upper()

    def generate_extraction_query(self, store_id: str, start_date: date, end_date: date) -> str:
        """Generates standard SQL query for POS sales extraction from BigQuery or Azure Synapse."""
        if self.target_cloud == "GCP":
            table_ref = "`wwnz-analytics-prod.retail_pos.store_daily_sales`"
        else:
            table_ref = "wwnz_azure_lakehouse.retail_pos.store_daily_sales"

        return f"""
SELECT
    store_id,
    product_sku,
    DATE(transaction_timestamp) AS sales_date,
    SUM(quantity) AS units_sold,
    AVG(unit_price_nzd) AS unit_price_nzd,
    MAX(CASE WHEN is_promo_active THEN 1 ELSE 0 END) AS is_promotional,
    MAX(inventory_on_hand_eod) AS inventory_on_hand
FROM {table_ref}
WHERE store_id = '{store_id}'
  AND DATE(transaction_timestamp) BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'
GROUP BY 1, 2, 3
ORDER BY sales_date ASC;
""".strip()

    def transform_and_validate(self, raw_rows: List[Dict[str, Any]]) -> List[StoreProductSales]:
        """Validates schemas, imputes missing values, and casts to typed domain objects."""
        validated = []
        for row in raw_rows:
            sales_date = row["sales_date"]
            if isinstance(sales_date, str):
                sales_date = date.fromisoformat(sales_date)

            sales = StoreProductSales(
                store_id=str(row["store_id"]),
                product_sku=str(row["product_sku"]),
                sales_date=sales_date,
                units_sold=max(0.0, float(row.get("units_sold", 0.0))),
                unit_price_nzd=max(0.0, float(row.get("unit_price_nzd", 0.0))),
                is_promotional=bool(row.get("is_promotional", False)),
                inventory_on_hand=max(0.0, float(row.get("inventory_on_hand", 0.0))),
            )
            validated.append(sales)
        return validated


class ResponsibleAIGovernance:
    """Responsible AI safeguards for customer privacy and algorithmic fairness."""

    @staticmethod
    def mask_customer_identifier(customer_id: str) -> str:
        """Applies SHA-256 masking with salt to protect customer PII."""
        import hashlib
        salt = "wwnz_privacy_salt_2026"
        hashed = hashlib.sha256(f"{salt}_{customer_id}".encode()).hexdigest()
        return f"ANON_{hashed[:12]}"

    @staticmethod
    def audit_pricing_fairness(
        base_price: float,
        discounted_price: float,
        max_discount_ratio: float = 0.40
    ) -> Dict[str, Any]:
        """Audits personalized offers to ensure compliance with fair trading rules."""
        if base_price <= 0:
            return {"fair": False, "reason": "Base price must be positive"}

        discount_ratio = (base_price - discounted_price) / base_price
        if discount_ratio > max_discount_ratio:
            return {
                "fair": False,
                "reason": f"Discount ratio {discount_ratio:.1%} exceeds maximum permissible {max_discount_ratio:.1%}",
                "adjusted_price": round(base_price * (1.0 - max_discount_ratio), 2)
            }

        return {
            "fair": True,
            "discount_ratio": round(discount_ratio, 4),
            "final_price": round(discounted_price, 2)
        }
