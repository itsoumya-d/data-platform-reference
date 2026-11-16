"""Unit tests for Bank of New Zealand (BNZ) Core Banking Data Lifecycle Engine."""

import pytest
from dataplat.bnz_lifecycle import (
    BNZDataLifecycleManager,
    ChangeSeverity,
    ColumnDefinition,
    DataLineageGraph,
    LifecycleStage,
    LineageNode,
    ReconciliationRootCauseAnalyzer,
    SchemaChange,
    SchemaDriftDetector,
    SchemaSnapshot,
    SourceToTargetMapping,
)


@pytest.fixture
def lending_v1_schema():
    return SchemaSnapshot(
        system_name="Hogan_Core_Banking",
        entity_name="LENDING_FACILITY",
        version="v1.0",
        columns={
            "facility_id": ColumnDefinition("facility_id", "VARCHAR(32)", is_nullable=False, is_critical_data_element=True),
            "customer_id": ColumnDefinition("customer_id", "VARCHAR(32)", is_nullable=False, is_critical_data_element=True),
            "drawn_amount": ColumnDefinition("drawn_amount", "DECIMAL(18,2)", is_nullable=False, is_critical_data_element=True),
            "interest_rate": ColumnDefinition("interest_rate", "DECIMAL(5,4)", is_nullable=False, is_critical_data_element=False),
            "origination_branch": ColumnDefinition("origination_branch", "VARCHAR(16)", is_nullable=True, is_critical_data_element=False),
        }
    )


@pytest.fixture
def lineage_graph():
    graph = DataLineageGraph()
    # Nodes across lifecycle stages
    src = LineageNode("src_hogan_lending", LifecycleStage.SOURCE_SYSTEM, "LENDING_FACILITY", ["facility_id", "customer_id", "drawn_amount"])
    stg = LineageNode("stg_lending", LifecycleStage.STAGING, "stg_hogan__lending_facilities", ["facility_id", "customer_id", "drawn_amount"])
    mart = LineageNode("fct_loan_exposure", LifecycleStage.ANALYTICAL_MART, "fct_core_loan_exposures", ["facility_id", "customer_id", "drawn_amount", "risk_weighted_exposure"])
    rbnz_bs11 = LineageNode("rpt_rbnz_bs11", LifecycleStage.REGULATORY_REPORTING, "rbnz_bs11_capital_adequacy", ["total_capital_exposure"], is_regulatory_target=True, regulatory_framework="RBNZ_BS11")
    bcbs_239 = LineageNode("rpt_bcbs239_cde", LifecycleStage.REGULATORY_REPORTING, "bcbs239_risk_aggregation", ["aggregate_credit_risk"], is_regulatory_target=True, regulatory_framework="BCBS_239")

    for n in [src, stg, mart, rbnz_bs11, bcbs_239]:
        graph.add_node(n)

    graph.add_edge("src_hogan_lending", "stg_lending")
    graph.add_edge("stg_lending", "fct_loan_exposure")
    graph.add_edge("fct_loan_exposure", "rpt_rbnz_bs11")
    graph.add_edge("fct_loan_exposure", "rpt_bcbs239_cde")
    return graph


def test_schema_drift_detector_detects_cde_removal(lending_v1_schema):
    # Create v2 with drawn_amount removed (critical data element)
    v2_cols = dict(lending_v1_schema.columns)
    del v2_cols["drawn_amount"]
    lending_v2 = SchemaSnapshot("Hogan_Core_Banking", "LENDING_FACILITY", "v2.0", v2_cols)

    changes = SchemaDriftDetector.compare_schemas(lending_v1_schema, lending_v2)
    assert len(changes) == 1
    ch = changes[0]
    assert ch.change_type == "REMOVED"
    assert ch.column_name == "drawn_amount"
    assert ch.severity == ChangeSeverity.CRITICAL_BREAKING
    assert ch.is_cde_affected is True


def test_schema_drift_detector_detects_type_alteration(lending_v1_schema):
    v2_cols = dict(lending_v1_schema.columns)
    # Type changed from DECIMAL(18,2) to VARCHAR(64)
    v2_cols["drawn_amount"] = ColumnDefinition("drawn_amount", "VARCHAR(64)", is_nullable=False, is_critical_data_element=True)
    lending_v2 = SchemaSnapshot("Hogan_Core_Banking", "LENDING_FACILITY", "v2.0", v2_cols)

    changes = SchemaDriftDetector.compare_schemas(lending_v1_schema, lending_v2)
    assert len(changes) == 1
    ch = changes[0]
    assert ch.change_type == "TYPE_CHANGED"
    assert ch.old_value == "DECIMAL(18,2)"
    assert ch.new_value == "VARCHAR(64)"
    assert ch.severity == ChangeSeverity.CRITICAL_BREAKING


def test_data_lineage_graph_traversal(lineage_graph):
    downstream = lineage_graph.trace_downstream("src_hogan_lending")
    node_ids = [n.node_id for n in downstream]
    assert "stg_lending" in node_ids
    assert "fct_loan_exposure" in node_ids
    assert "rpt_rbnz_bs11" in node_ids
    assert "rpt_bcbs239_cde" in node_ids


def test_impacted_regulatory_reports_identification(lineage_graph):
    reg_reports = lineage_graph.get_impacted_regulatory_reports("src_hogan_lending")
    frameworks = {r.regulatory_framework for r in reg_reports}
    assert "RBNZ_BS11" in frameworks
    assert "BCBS_239" in frameworks


def test_reconciliation_root_cause_balanced():
    result = ReconciliationRootCauseAnalyzer.diagnose_imbalance(
        source_balance=50_000_000.00,
        staged_balance=50_000_000.00,
        mart_balance=50_000_000.00,
        report_balance=50_000_000.00
    )
    assert result["status"] == "BALANCED"
    assert result["discrepancy"] == 0.0


def test_reconciliation_root_cause_ingestion_drop():
    result = ReconciliationRootCauseAnalyzer.diagnose_imbalance(
        source_balance=50_000_000.00,
        staged_balance=48_500_000.00,
        mart_balance=48_500_000.00,
        report_balance=48_500_000.00
    )
    assert result["status"] == "IMBALANCED"
    assert result["failing_stage"] == LifecycleStage.INGESTION.value
    assert "CDC" in result["remedy"]


def test_reconciliation_root_cause_dbt_transformation_drop():
    result = ReconciliationRootCauseAnalyzer.diagnose_imbalance(
        source_balance=50_000_000.00,
        staged_balance=50_000_000.00,
        mart_balance=47_200_000.00,
        report_balance=47_200_000.00
    )
    assert result["status"] == "IMBALANCED"
    assert result["failing_stage"] == LifecycleStage.TRANSFORMATION_DBT.value
    assert "dbt model join predicates" in result["remedy"]


def test_bnz_lifecycle_manager_evaluate_release_impact(lending_v1_schema, lineage_graph):
    manager = BNZDataLifecycleManager(lineage_graph)
    # New schema with added attribute and altered nullable
    v2_cols = dict(lending_v1_schema.columns)
    v2_cols["loan_security_type"] = ColumnDefinition("loan_security_type", "VARCHAR(32)", is_nullable=True, is_critical_data_element=True)
    lending_v2 = SchemaSnapshot("Hogan_Core_Banking", "LENDING_FACILITY", "v2.0", v2_cols)

    impact = manager.evaluate_release_impact(lending_v1_schema, lending_v2, "src_hogan_lending")
    assert impact["source_system"] == "Hogan_Core_Banking"
    assert impact["total_changes"] == 1
    assert impact["cde_impacted"] is True
    assert "RBNZ_BS11" in impact["impacted_regulatory_frameworks"]
    assert "BCBS_239" in impact["impacted_regulatory_frameworks"]
