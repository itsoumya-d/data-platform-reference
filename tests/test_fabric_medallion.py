"""Tests for Microsoft Fabric Medallion Lakehouse, Lineage & Agent Retrieval."""
from dataplat.fabric_medallion import (
    AgentRetrievalConnector,
    DirectLakeSemanticModel,
    FabricCapacityMonitor,
    LineageGraphTracker,
    MedallionLakehouse,
)


def test_medallion_lakehouse_pipeline():
    lakehouse = MedallionLakehouse("test-workspace")

    # Ingest bronze raw data
    raw_parts = [
        {"part_id": "P-101", "name": "Titanium Spar", "cost": 1200.0, "facility": "Hengelo"},
        {"part_id": "P-102", "name": "Hydrogen Valve", "cost": 450.0, "facility": "Hengelo"},
        {"part_id": "P-103", "name": "Wing Spar", "cost": 850.0, "facility": "Payerne"},
        {"part_id": "P-101", "name": "Titanium Spar v2", "cost": 1200.0, "facility": "Hengelo"},  # Duplicate
    ]

    bronze_res = lakehouse.ingest_bronze("aerospace_parts", raw_parts)
    assert bronze_res.records_committed == 4
    assert bronze_res.layer == "bronze"

    # Promote to silver with auto-schema evolution and deduplication
    silver_res = lakehouse.promote_to_silver(
        source_table="aerospace_parts",
        target_table="dim_parts",
        primary_key="part_id",
        auto_evolve=True,
    )
    assert silver_res.records_committed == 3  # P-101 deduplicated
    assert silver_res.duplicates_dropped == 1
    assert silver_res.schema_version >= 1

    # Verify silver schema contains evolved columns
    silver_schema = lakehouse.get_table("silver", "dim_parts")
    assert silver_schema is not None
    cols = silver_schema.column_map()
    assert "cost" in cols
    assert "facility" in cols

    # Publish gold aggregated KPIs
    gold_res = lakehouse.publish_gold_aggregation(
        silver_table="dim_parts",
        gold_table="facility_cost_summary",
        group_by_key="facility",
        metric_column="cost",
    )
    assert gold_res.records_committed == 2  # Hengelo and Payerne
    gold_records = lakehouse.get_gold_records("facility_cost_summary")
    hengelo = next(r for r in gold_records if r["facility"] == "Hengelo")
    assert hengelo["total_cost"] == 1650.0
    assert hengelo["record_count"] == 2


def test_lineage_graph_tracker():
    tracker = LineageGraphTracker()
    tracker.add_lineage("erp.dynamics365_parts", "bronze.aerospace_parts")
    tracker.add_lineage("bronze.aerospace_parts", "silver.dim_parts")
    tracker.add_lineage("silver.dim_parts", "gold.facility_cost_summary")
    tracker.add_lineage("gold.facility_cost_summary", "powerbi.executive_dashboard")

    # Upstream check
    upstream = tracker.upstream_dependencies("powerbi.executive_dashboard")
    assert "erp.dynamics365_parts" in upstream
    assert "bronze.aerospace_parts" in upstream
    assert "silver.dim_parts" in upstream
    assert "gold.facility_cost_summary" in upstream

    # Downstream impact analysis
    impact = tracker.downstream_impact("erp.dynamics365_parts")
    assert "powerbi.executive_dashboard" in impact

    # Export Purview manifest
    manifest = tracker.export_purview_manifest()
    assert manifest["entityCount"] == 5
    assert manifest["lineageRelationshipCount"] == 4


def test_direct_lake_semantic_model():
    model = DirectLakeSemanticModel(
        model_name="AerospaceOperationsSemanticModel",
        target_gold_table="gold.facility_cost_summary",
        dimensions=["facility", "part_category"],
        measures=["total_cost", "avg_cost", "record_count"],
    )
    definition = model.to_power_bi_definition()
    assert definition["mode"] == "DirectLake"
    assert definition["sourceTable"] == "gold.facility_cost_summary"
    assert "facility" in definition["dimensions"]


def test_agent_retrieval_connector():
    lakehouse = MedallionLakehouse("agent-space")
    tracker = LineageGraphTracker()
    tracker.add_lineage("bronze.uav_components", "silver.clean_components")
    tracker.add_lineage("silver.clean_components", "gold.uav_inventory")

    # Setup dummy gold data
    lakehouse._gold_storage["uav_inventory"] = [
        {"program": "Hypersonic-UAV", "shortages": 0, "build_progress_pct": 88.5},
        {"program": "Subsonic-Cargo", "shortages": 3, "build_progress_pct": 62.0},
    ]

    connector = AgentRetrievalConnector(lakehouse, tracker)
    tools = connector.get_tool_definitions()
    assert len(tools) == 2
    assert tools[0]["name"] == "query_gold_metrics"

    # Query without filter
    res = connector.execute_agent_query("uav_inventory")
    assert res["query_status"] == "success"
    assert res["match_count"] == 2
    assert "silver.clean_components" in res["provenance_upstream"]

    # Query with filter
    filtered = connector.execute_agent_query("uav_inventory", "program", "Hypersonic-UAV")
    assert filtered["match_count"] == 1
    assert filtered["results"][0]["build_progress_pct"] == 88.5


def test_fabric_capacity_monitor():
    monitor = FabricCapacityMonitor(capacity_sku="F64", max_hourly_cu=64.0)

    # Record normal job
    normal = monitor.record_job("job-1", "spark_notebook", cu_seconds=3600.0, duration_seconds=600.0)
    assert not normal["is_runaway"]

    # Record runaway job (e.g. 500,000 CU seconds = ~138 CU/hr)
    runaway = monitor.record_job("job-2", "spark_notebook", cu_seconds=500000.0, duration_seconds=1200.0)
    assert runaway["is_runaway"]

    assert len(monitor.runaway_jobs()) == 1
    assert monitor.calculate_total_capacity_burn() > 100.0
