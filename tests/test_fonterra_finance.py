from decimal import Decimal

from dataplat.fonterra_finance import (
    build_account_dimension,
    build_invoice_fact,
    build_supplier_dimension,
    finance_metrics,
    lineage_edges,
    money,
    reconcile,
    source_to_target,
    source_to_target_count,
    validate_rows,
)

ROWS = [
    {"invoice_id": "I1", "supplier_id": "S1", "supplier_name": "Fonterra", "country": "nz", "account_id": "A1", "account_name": "Milk", "account_type": "Expense", "invoice_date": "2026-11-01", "amount": "100.00", "tax": "15.00", "currency": "nzd"},
    {"invoice_id": "I2", "supplier_id": "S2", "supplier_name": "Retail Co", "country": "au", "account_id": "A2", "account_name": "Freight", "account_type": "Expense", "invoice_date": "2026-11-02", "amount": "200.00", "tax": "30.00", "currency": "aud"},
]
MAPPING = {"invoice_id": "invoice_id", "supplier_id": "supplier_id", "amount": "amount"}


def test_money_rounds():
    assert money("10.005") == Decimal("10.01")


def test_source_to_target_maps_fields():
    assert source_to_target(ROWS, MAPPING)[0]["invoice_id"] == "I1"


def test_source_to_target_count():
    assert source_to_target_count(ROWS, MAPPING) == 2


def test_missing_source_is_none():
    assert source_to_target(ROWS, {"x": "missing"})[0]["x"] is None


def test_supplier_dimension_dedupes():
    assert len(build_supplier_dimension(ROWS + [ROWS[0]])) == 2


def test_supplier_key():
    assert build_supplier_dimension(ROWS)[0]["supplier_key"] == "SUP-S1"


def test_supplier_country_uppercase():
    assert build_supplier_dimension(ROWS)[0]["country"] == "NZ"


def test_supplier_ignores_blank_id():
    assert len(build_supplier_dimension([{"supplier_id": ""}])) == 0


def test_account_dimension_dedupes():
    assert len(build_account_dimension(ROWS + [ROWS[0]])) == 2


def test_account_type_lowercase():
    assert build_account_dimension(ROWS)[0]["account_type"] == "expense"


def test_account_key():
    assert build_account_dimension(ROWS)[0]["account_key"] == "ACC-A1"


def test_invoice_fact_rows():
    assert len(build_invoice_fact(ROWS)) == 2


def test_invoice_total_includes_tax():
    assert build_invoice_fact(ROWS)[0]["total"] == Decimal("115.00")


def test_invoice_key():
    assert build_invoice_fact(ROWS)[0]["invoice_key"] == "INV-I1"


def test_invoice_currency_uppercase():
    assert build_invoice_fact(ROWS)[0]["currency"] == "NZD"


def test_invoice_skips_missing_id():
    assert len(build_invoice_fact([{"invoice_id": ""}])) == 0


def test_reconcile_equal():
    assert reconcile("100", "100")["status"] == "reconciled"


def test_reconcile_within_tolerance():
    assert reconcile("100", "100.01")["status"] == "reconciled"


def test_reconcile_exception():
    assert reconcile("100", "100.02")["status"] == "exception"


def test_reconcile_difference():
    assert reconcile("100", "90")["difference"] == Decimal("10.00")


def test_reconcile_evidence():
    assert "tolerance" in reconcile("100", "90")["evidence"]


def test_validate_clean():
    assert validate_rows(ROWS) == []


def test_validate_missing_invoice():
    assert "missing invoice_id" in validate_rows([{"supplier_id": "S1", "amount": 1, "tax": 0, "currency": "NZD"}])[0]


def test_validate_duplicate_invoice():
    assert any("duplicate" in issue for issue in validate_rows(ROWS + [ROWS[0]]))


def test_validate_missing_supplier():
    assert any("supplier_id" in issue for issue in validate_rows([{"invoice_id": "I1", "amount": 1, "tax": 0, "currency": "NZD"}]))


def test_validate_negative_amount():
    assert any("negative amount" in issue for issue in validate_rows([{"invoice_id": "I1", "supplier_id": "S1", "amount": -1, "tax": 0, "currency": "NZD"}]))


def test_validate_negative_tax():
    assert any("negative tax" in issue for issue in validate_rows([{"invoice_id": "I1", "supplier_id": "S1", "amount": 1, "tax": -1, "currency": "NZD"}]))


def test_validate_currency():
    assert any("unsupported currency" in issue for issue in validate_rows([{"invoice_id": "I1", "supplier_id": "S1", "amount": 1, "tax": 0, "currency": "GBP"}]))


def test_lineage_has_gold_edges():
    assert ("silver.invoices", "gold.fact_invoice") in lineage_edges()


def test_lineage_count():
    assert len(lineage_edges()) == 9


def test_finance_metrics_totals():
    metrics = finance_metrics(ROWS)
    assert metrics["amount"] == Decimal("300.00")
    assert metrics["tax"] == Decimal("45.00")
    assert metrics["total"] == Decimal("345.00")
