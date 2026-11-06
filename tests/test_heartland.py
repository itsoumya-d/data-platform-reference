import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from dataplat.generate import generate
from dataplat.heartland import (
    common_equity_tier1_ratio,
    daily_net_flow,
    deposit_lending_split,
    hashdiff,
    hub_account_key,
    hub_customer_key,
    link_customer_account_key,
    liquidity_coverage_ratio,
    net_interest_margin,
    regulatory_summary,
    validate_adf_pipeline,
)


def tx(amount, direction, status="posted", account="A1", ts="2026-11-05T10:00:00"):
    return {"account_id": account, "amount_nzd": amount, "direction": direction, "status": status, "transaction_ts": datetime.fromisoformat(ts)}


def test_customer_hub_key_is_stable():
    assert hub_customer_key("C00001") == hub_customer_key(" c00001 ")


def test_account_hub_keys_are_distinct():
    assert hub_account_key("A1") != hub_account_key("A2")


def test_link_key_is_order_stable():
    assert link_customer_account_key("C1", "A1") == link_customer_account_key("c1", "a1")


def test_hashdiff_is_key_order_stable():
    assert hashdiff(segment="smb", country="NZ") == hashdiff(country="NZ", segment="smb")


def test_hashdiff_changes_with_value():
    assert hashdiff(segment="smb") != hashdiff(segment="enterprise")


def test_daily_net_flow_credit():
    assert daily_net_flow([tx(100, "credit")])[(date(2026, 11, 5), "A1")] == Decimal("100")


def test_daily_net_flow_debit():
    assert daily_net_flow([tx(40, "debit")])[(date(2026, 11, 5), "A1")] == Decimal("-40")


def test_daily_net_flow_skips_pending():
    assert daily_net_flow([tx(100, "credit", status="pending")]) == {}


def test_daily_net_flow_groups_accounts():
    flows = daily_net_flow([tx(100, "credit", account="A1"), tx(25, "debit", account="A2")])
    assert flows[(date(2026, 11, 5), "A1")] == Decimal("100")
    assert flows[(date(2026, 11, 5), "A2")] == Decimal("-25")


def test_net_interest_margin():
    assert net_interest_margin(100, 40, 1000) == Decimal("0.0600")


def test_cet1_ratio():
    assert common_equity_tier1_ratio(120, 1000) == Decimal("0.1200")


def test_liquidity_coverage_ratio():
    assert liquidity_coverage_ratio(150, 100) == Decimal("1.5000")


def test_ratio_rejects_zero_denominator():
    with pytest.raises(ValueError):
        common_equity_tier1_ratio(120, 0)


def test_regulatory_summary():
    summary = regulatory_summary(100, 40, 1000, 120, 1000, 150, 100)
    assert summary.net_interest_margin == Decimal("0.0600")
    assert summary.cet1_ratio == Decimal("0.1200")
    assert summary.liquidity_coverage_ratio == Decimal("1.5000")


def test_deposit_lending_split():
    accounts = [
        {"account_type": "savings", "status": "active", "balance_nzd": 100},
        {"account_type": "transaction", "status": "active", "balance_nzd": 50},
        {"account_type": "loan", "status": "active", "balance_nzd": 400},
        {"account_type": "loan", "status": "closed", "balance_nzd": 999},
    ]
    assert deposit_lending_split(accounts) == {"deposits": Decimal("150.00"), "lending": Decimal("400.00")}


def test_adf_pipeline_contract():
    path = Path("adf/pipeline_ingest_core_banking.json")
    pipeline = json.loads(path.read_text())
    assert validate_adf_pipeline(pipeline) == []


def test_adf_pipeline_rejects_missing_copy():
    assert "Copy activity" in validate_adf_pipeline({"name": "p", "properties": {"parameters": {}, "activities": []}})


def test_banking_generation_adds_tables(tmp_path):
    paths = generate(tmp_path, rows=300, seed=9)
    accounts = pd.read_parquet(paths["accounts"])
    transactions = pd.read_parquet(paths["transactions"])
    assert len(accounts) == 700
    assert len(transactions) == 1200
    assert set(accounts["account_type"]) <= {"transaction", "savings", "loan"}
    assert set(transactions["status"]) <= {"posted", "pending"}
