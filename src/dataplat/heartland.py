"""Heartland Bank data-vault and regulatory metric helpers.

The functions are deliberately pure so the banking rules can be tested without
Snowflake, Azure Data Factory or AtScale.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Iterable, Mapping

ZERO = Decimal("0")


def _digest(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:32]


def hub_customer_key(customer_id: str) -> str:
    return _digest("hub_customer", customer_id.strip().upper())


def hub_account_key(account_id: str) -> str:
    return _digest("hub_account", account_id.strip().upper())


def link_customer_account_key(customer_id: str, account_id: str) -> str:
    return _digest(
        "link_customer_account",
        customer_id.strip().upper(),
        account_id.strip().upper(),
    )


def hashdiff(**fields: object) -> str:
    canonical = json.dumps(fields, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def daily_net_flow(transactions: Iterable[Mapping[str, object]]) -> dict[tuple[date, str], Decimal]:
    """Group posted transactions into a signed daily flow per account."""
    flows: dict[tuple[date, str], Decimal] = defaultdict(lambda: ZERO)
    for row in transactions:
        if str(row.get("status", "")).lower() != "posted":
            continue
        ts = row["transaction_ts"]
        if isinstance(ts, str):
            day = date.fromisoformat(ts[:10])
        else:
            day = ts.date()  # type: ignore[union-attr]
        amount = Decimal(str(row["amount_nzd"]))
        signed = amount if str(row["direction"]).lower() == "credit" else -amount
        flows[(day, str(row["account_id"]))] += signed
    return dict(flows)


def _ratio(numerator: Decimal | int | str, denominator: Decimal | int | str) -> Decimal:
    try:
        bottom = Decimal(str(denominator))
        if bottom <= 0:
            raise ValueError("denominator must be positive")
        return (Decimal(str(numerator)) / bottom).quantize(Decimal("0.0001"))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("ratio inputs must be numeric") from exc


def net_interest_margin(
    interest_income: Decimal | int | str,
    interest_expense: Decimal | int | str,
    average_earning_assets: Decimal | int | str,
) -> Decimal:
    return _ratio(Decimal(str(interest_income)) - Decimal(str(interest_expense)), average_earning_assets)


def common_equity_tier1_ratio(
    cet1_capital: Decimal | int | str,
    risk_weighted_assets: Decimal | int | str,
) -> Decimal:
    return _ratio(cet1_capital, risk_weighted_assets)


def liquidity_coverage_ratio(
    high_quality_liquid_assets: Decimal | int | str,
    net_cash_outflows: Decimal | int | str,
) -> Decimal:
    return _ratio(high_quality_liquid_assets, net_cash_outflows)


@dataclass(frozen=True)
class RegulatorySummary:
    net_interest_margin: Decimal
    cet1_ratio: Decimal
    liquidity_coverage_ratio: Decimal


def regulatory_summary(
    interest_income: Decimal | int | str,
    interest_expense: Decimal | int | str,
    average_earning_assets: Decimal | int | str,
    cet1_capital: Decimal | int | str,
    risk_weighted_assets: Decimal | int | str,
    high_quality_liquid_assets: Decimal | int | str,
    net_cash_outflows: Decimal | int | str,
) -> RegulatorySummary:
    return RegulatorySummary(
        net_interest_margin=net_interest_margin(interest_income, interest_expense, average_earning_assets),
        cet1_ratio=common_equity_tier1_ratio(cet1_capital, risk_weighted_assets),
        liquidity_coverage_ratio=liquidity_coverage_ratio(high_quality_liquid_assets, net_cash_outflows),
    )


def deposit_lending_split(accounts: Iterable[Mapping[str, object]]) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = defaultdict(lambda: ZERO)
    for account in accounts:
        if str(account.get("status", "")).lower() != "active":
            continue
        account_type = str(account["account_type"]).lower()
        bucket = "lending" if account_type == "loan" else "deposits"
        totals[bucket] += Decimal(str(account.get("balance_nzd", 0)))
    return {"deposits": totals["deposits"].quantize(Decimal("0.01")), "lending": totals["lending"].quantize(Decimal("0.01"))}


def validate_adf_pipeline(pipeline: Mapping[str, object]) -> list[str]:
    """Return the missing ADF contract fields; an empty list means valid."""
    missing: list[str] = []
    props = pipeline.get("properties")
    if not pipeline.get("name"):
        missing.append("name")
    if not isinstance(props, Mapping):
        return missing + ["properties"]
    if not props.get("parameters"):
        missing.append("properties.parameters")
    activities = props.get("activities")
    if not isinstance(activities, list) or not activities:
        missing.append("properties.activities")
        missing.append("Copy activity")
    else:
        if not any(activity.get("type") == "Copy" for activity in activities if isinstance(activity, Mapping)):
            missing.append("Copy activity")
        for activity in activities:
            if isinstance(activity, Mapping) and activity.get("type") == "Copy":
                copy_props = activity.get("typeProperties", {})
                if not copy_props.get("source") or not copy_props.get("sink"):
                    missing.append("Copy source/sink")
                    break
    return missing
