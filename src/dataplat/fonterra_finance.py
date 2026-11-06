"""Fonterra-shaped finance data product helpers.

Pure functions keep source-to-target mapping, dimensional modelling,
reconciliation and lineage testable outside Databricks.
"""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable, Mapping, Sequence

MONEY = Decimal("0.01")


def money(value: Decimal | int | float | str) -> Decimal:
    return Decimal(str(value)).quantize(MONEY, rounding=ROUND_HALF_UP)


def source_to_target(rows: Iterable[Mapping[str, object]], mapping: Mapping[str, str]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for row in rows:
        out.append({target: row.get(source) for target, source in mapping.items()})
    return out


def build_supplier_dimension(rows: Iterable[Mapping[str, object]]) -> list[dict[str, object]]:
    seen: dict[str, dict[str, object]] = {}
    for row in rows:
        supplier_id = str(row.get("supplier_id") or "").strip()
        if not supplier_id:
            continue
        seen[supplier_id] = {
            "supplier_key": f"SUP-{supplier_id}",
            "supplier_id": supplier_id,
            "supplier_name": str(row.get("supplier_name") or "").strip(),
            "country": str(row.get("country") or "").upper(),
        }
    return sorted(seen.values(), key=lambda item: str(item["supplier_id"]))


def build_account_dimension(rows: Iterable[Mapping[str, object]]) -> list[dict[str, object]]:
    seen: dict[str, dict[str, object]] = {}
    for row in rows:
        account_id = str(row.get("account_id") or "").strip()
        if not account_id:
            continue
        seen[account_id] = {
            "account_key": f"ACC-{account_id}",
            "account_id": account_id,
            "account_name": str(row.get("account_name") or "").strip(),
            "account_type": str(row.get("account_type") or "expense").lower(),
        }
    return sorted(seen.values(), key=lambda item: str(item["account_id"]))


def build_invoice_fact(rows: Iterable[Mapping[str, object]]) -> list[dict[str, object]]:
    fact: list[dict[str, object]] = []
    for row in rows:
        invoice_id = str(row.get("invoice_id") or "").strip()
        if not invoice_id:
            continue
        amount = money(row.get("amount") or 0)
        tax = money(row.get("tax") or 0)
        fact.append(
            {
                "invoice_key": f"INV-{invoice_id}",
                "invoice_id": invoice_id,
                "supplier_key": f"SUP-{str(row.get('supplier_id') or '').strip()}",
                "account_key": f"ACC-{str(row.get('account_id') or '').strip()}",
                "invoice_date": str(row.get("invoice_date") or ""),
                "amount": amount,
                "tax": tax,
                "total": money(amount + tax),
                "currency": str(row.get("currency") or "NZD").upper(),
            }
        )
    return sorted(fact, key=lambda item: str(item["invoice_id"]))


def reconcile(gl_total: Decimal | str | int | float, subledger_total: Decimal | str | int | float, tolerance: Decimal | str | int | float = "0.01") -> dict[str, object]:
    gl = money(gl_total)
    subledger = money(subledger_total)
    difference = money(gl - subledger)
    tolerance_value = money(tolerance)
    status = "reconciled" if abs(difference) <= tolerance_value else "exception"
    return {
        "gl_total": gl,
        "subledger_total": subledger,
        "difference": difference,
        "tolerance": tolerance_value,
        "status": status,
        "evidence": f"GL {gl} minus subledger {subledger} = {difference}; tolerance {tolerance_value}.",
    }


def validate_rows(rows: Sequence[Mapping[str, object]]) -> list[str]:
    issues: list[str] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        invoice_id = str(row.get("invoice_id") or "").strip()
        if not invoice_id:
            issues.append(f"row {index}: missing invoice_id")
        elif invoice_id in seen:
            issues.append(f"row {index}: duplicate invoice_id {invoice_id}")
        else:
            seen.add(invoice_id)
        if not str(row.get("supplier_id") or "").strip():
            issues.append(f"row {index}: missing supplier_id")
        if money(row.get("amount") or 0) < 0:
            issues.append(f"row {index}: negative amount")
        if money(row.get("tax") or 0) < 0:
            issues.append(f"row {index}: negative tax")
        if str(row.get("currency") or "").upper() not in {"NZD", "AUD", "USD"}:
            issues.append(f"row {index}: unsupported currency")
    return issues


def lineage_edges() -> list[tuple[str, str]]:
    return [
        ("source.invoices", "bronze.invoices"),
        ("source.suppliers", "bronze.suppliers"),
        ("source.accounts", "bronze.accounts"),
        ("bronze.invoices", "silver.invoices"),
        ("bronze.suppliers", "silver.suppliers"),
        ("bronze.accounts", "silver.accounts"),
        ("silver.invoices", "gold.fact_invoice"),
        ("silver.suppliers", "gold.dim_supplier"),
        ("silver.accounts", "gold.dim_account"),
    ]


def finance_metrics(rows: Sequence[Mapping[str, object]]) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for row in rows:
        amount = money(row.get("amount") or 0)
        tax = money(row.get("tax") or 0)
        totals["amount"] += amount
        totals["tax"] += tax
        totals["total"] += amount + tax
        totals[str(row.get("currency") or "NZD").upper()] += amount + tax
    return {key: money(value) for key, value in totals.items()}


def source_to_target_count(rows: Iterable[Mapping[str, object]], mapping: Mapping[str, str]) -> int:
    return len(source_to_target(rows, mapping))
