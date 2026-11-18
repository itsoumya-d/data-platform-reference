"""Microsoft Fabric Medallion Lakehouse Engine, Lineage Governance & Agent Retrieval.

Implements OneLake-style Bronze/Silver/Gold medallion pipelines, Delta Lake schema
evolution, Purview-compatible lineage tracking, Direct Lake semantic model abstractions,
and grounded data retrieval connectors for enterprise AI agents.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import time
from typing import Any


@dataclasses.dataclass(frozen=True)
class DeltaColumn:
    name: str
    data_type: str  # "string", "int", "float", "boolean", "timestamp"
    nullable: bool = True


@dataclasses.dataclass
class DeltaTableSchema:
    table_name: str
    layer: str  # "bronze", "silver", "gold"
    columns: list[DeltaColumn]
    partition_keys: list[str] = dataclasses.field(default_factory=list)
    version: int = 1

    def column_map(self) -> dict[str, DeltaColumn]:
        return {c.name: c for c in self.columns}

    def evolve_schema(self, new_columns: list[DeltaColumn]) -> None:
        existing = self.column_map()
        for col in new_columns:
            if col.name not in existing:
                self.columns.append(col)
                self.version += 1


@dataclasses.dataclass
class IngestBatchResult:
    batch_id: str
    table_name: str
    layer: str
    records_received: int
    records_committed: int
    duplicates_dropped: int
    schema_version: int
    checksum: str


class MedallionLakehouse:
    """Manages Delta Lakehouse tables across Bronze, Silver, and Gold tiers."""

    def __init__(self, workspace_name: str = "production-estate") -> None:
        self.workspace_name = workspace_name
        self._tables: dict[str, DeltaTableSchema] = {}
        self._bronze_storage: dict[str, list[dict[str, Any]]] = {}
        self._silver_storage: dict[str, dict[str, dict[str, Any]]] = {}  # pk -> record
        self._gold_storage: dict[str, list[dict[str, Any]]] = {}

    def register_table(
        self,
        table_name: str,
        layer: str,
        columns: list[DeltaColumn],
        partition_keys: list[str] | None = None,
    ) -> DeltaTableSchema:
        schema = DeltaTableSchema(
            table_name=table_name,
            layer=layer,
            columns=columns,
            partition_keys=partition_keys or [],
        )
        self._tables[f"{layer}.{table_name}"] = schema
        return schema

    def get_table(self, layer: str, table_name: str) -> DeltaTableSchema | None:
        return self._tables.get(f"{layer}.{table_name}")

    def ingest_bronze(self, table_name: str, records: list[dict[str, Any]]) -> IngestBatchResult:
        """Ingests raw records into Bronze storage with change audit metadata."""
        if not records:
            return IngestBatchResult("empty", table_name, "bronze", 0, 0, 0, 1, "")

        key = f"bronze.{table_name}"
        if key not in self._tables:
            self.register_table(
                table_name,
                "bronze",
                [DeltaColumn("raw_payload", "string"), DeltaColumn("ingested_at", "timestamp")],
            )

        stored = self._bronze_storage.setdefault(table_name, [])
        now = time.time()
        batch_hasher = hashlib.sha256()

        for rec in records:
            payload_str = json.dumps(rec, sort_keys=True)
            batch_hasher.update(payload_str.encode("utf-8"))
            stored.append({"payload": rec, "ingested_at": now})

        schema = self._tables[key]
        return IngestBatchResult(
            batch_id=f"batch-{int(now * 1000)}",
            table_name=table_name,
            layer="bronze",
            records_received=len(records),
            records_committed=len(records),
            duplicates_dropped=0,
            schema_version=schema.version,
            checksum=batch_hasher.hexdigest(),
        )

    def promote_to_silver(
        self,
        source_table: str,
        target_table: str,
        primary_key: str,
        auto_evolve: bool = True,
    ) -> IngestBatchResult:
        """Cleans, validates types, deduplicates by primary key, and evolves Silver schema."""
        raw_items = self._bronze_storage.get(source_table, [])
        silver_store = self._silver_storage.setdefault(target_table, {})

        key = f"silver.{target_table}"
        schema = self._tables.get(key)
        if not schema:
            schema = self.register_table(
                target_table,
                "silver",
                [DeltaColumn(primary_key, "string", nullable=False)],
            )

        committed = 0
        duplicates = 0
        discovered_cols: dict[str, DeltaColumn] = {}

        for item in raw_items:
            rec = item["payload"]
            pk_val = str(rec.get(primary_key, ""))
            if not pk_val:
                continue

            if auto_evolve:
                for k, v in rec.items():
                    if k not in schema.column_map():
                        dtype = "int" if isinstance(v, int) else "float" if isinstance(v, float) else "string"
                        discovered_cols[k] = DeltaColumn(name=k, data_type=dtype)

            if pk_val in silver_store:
                duplicates += 1
            else:
                committed += 1

            silver_store[pk_val] = rec

        if auto_evolve and discovered_cols:
            schema.evolve_schema(list(discovered_cols.values()))

        checksum = hashlib.sha256(str(len(silver_store)).encode()).hexdigest()
        return IngestBatchResult(
            batch_id=f"silver-{int(time.time())}",
            table_name=target_table,
            layer="silver",
            records_received=len(raw_items),
            records_committed=committed,
            duplicates_dropped=duplicates,
            schema_version=schema.version,
            checksum=checksum,
        )

    def publish_gold_aggregation(
        self,
        silver_table: str,
        gold_table: str,
        group_by_key: str,
        metric_column: str,
    ) -> IngestBatchResult:
        """Aggregates verified Silver records into governed Gold dimensional metrics."""
        silver_store = self._silver_storage.get(silver_table, {})
        sums: dict[str, float] = {}
        counts: dict[str, int] = {}

        for rec in silver_store.values():
            grp = str(rec.get(group_by_key, "unknown"))
            val = float(rec.get(metric_column, 0.0))
            sums[grp] = sums.get(grp, 0.0) + val
            counts[grp] = counts.get(grp, 0) + 1

        gold_records = []
        for grp, total in sums.items():
            cnt = counts[grp]
            gold_records.append(
                {
                    group_by_key: grp,
                    f"total_{metric_column}": round(total, 2),
                    "record_count": cnt,
                    f"avg_{metric_column}": round(total / cnt, 2) if cnt else 0.0,
                }
            )

        self._gold_storage[gold_table] = gold_records
        schema = self._tables.get(f"gold.{gold_table}")
        if not schema:
            schema = self.register_table(
                gold_table,
                "gold",
                [
                    DeltaColumn(group_by_key, "string", nullable=False),
                    DeltaColumn(f"total_{metric_column}", "float"),
                    DeltaColumn("record_count", "int"),
                    DeltaColumn(f"avg_{metric_column}", "float"),
                ],
            )

        return IngestBatchResult(
            batch_id=f"gold-{int(time.time())}",
            table_name=gold_table,
            layer="gold",
            records_received=len(silver_store),
            records_committed=len(gold_records),
            duplicates_dropped=0,
            schema_version=schema.version,
            checksum=hashlib.sha256(json.dumps(gold_records, sort_keys=True).encode()).hexdigest(),
        )

    def get_gold_records(self, gold_table: str) -> list[dict[str, Any]]:
        return list(self._gold_storage.get(gold_table, []))


class LineageGraphTracker:
    """Tracks end-to-end dataset and transformation dependencies across OneLake."""

    def __init__(self) -> None:
        self._nodes: set[str] = set()
        self._edges: set[tuple[str, str]] = set()  # (upstream, downstream)

    def add_dataset(self, name: str) -> None:
        self._nodes.add(name)

    def add_lineage(self, upstream: str, downstream: str) -> None:
        self._nodes.add(upstream)
        self._nodes.add(downstream)
        self._edges.add((upstream, downstream))

    def upstream_dependencies(self, dataset: str) -> list[str]:
        results: set[str] = set()
        queue = [dataset]
        while queue:
            curr = queue.pop(0)
            for up, down in self._edges:
                if down == curr and up not in results:
                    results.add(up)
                    queue.append(up)
        return sorted(results)

    def downstream_impact(self, dataset: str) -> list[str]:
        results: set[str] = set()
        queue = [dataset]
        while queue:
            curr = queue.pop(0)
            for up, down in self._edges:
                if up == curr and down not in results:
                    results.add(down)
                    queue.append(down)
        return sorted(results)

    def export_purview_manifest(self) -> dict[str, Any]:
        """Exports Purview-compatible lineage entity definitions."""
        return {
            "entityCount": len(self._nodes),
            "lineageRelationshipCount": len(self._edges),
            "entities": sorted(self._nodes),
            "relationships": [{"from": up, "to": down} for up, down in sorted(self._edges)],
        }


@dataclasses.dataclass
class DirectLakeSemanticModel:
    model_name: str
    target_gold_table: str
    dimensions: list[str]
    measures: list[str]
    storage_mode: str = "DirectLake"

    def to_power_bi_definition(self) -> dict[str, Any]:
        return {
            "name": self.model_name,
            "mode": self.storage_mode,
            "sourceTable": self.target_gold_table,
            "dimensions": self.dimensions,
            "measures": self.measures,
        }


class AgentRetrievalConnector:
    """Grounds conversational data agents and Company Brain knowledge tools in Gold data."""

    def __init__(self, lakehouse: MedallionLakehouse, lineage: LineageGraphTracker) -> None:
        self.lakehouse = lakehouse
        self.lineage = lineage

    def get_tool_definitions(self) -> list[dict[str, Any]]:
        """Returns OpenAPI/JSON tool schemas for autonomous agent tool calling."""
        return [
            {
                "name": "query_gold_metrics",
                "description": "Queries governed Gold aggregate metrics and production KPIs from OneLake.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "table_name": {"type": "string", "description": "Gold table name to query"},
                        "filter_key": {"type": "string", "description": "Key column to match"},
                        "filter_value": {"type": "string", "description": "Value to match"},
                    },
                    "required": ["table_name"],
                },
            },
            {
                "name": "explain_data_lineage",
                "description": "Explains upstream origins and data transformations feeding any reporting table.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "dataset_name": {"type": "string", "description": "Target dataset name"},
                    },
                    "required": ["dataset_name"],
                },
            },
        ]

    def execute_agent_query(
        self,
        table_name: str,
        filter_key: str | None = None,
        filter_value: str | None = None,
    ) -> dict[str, Any]:
        records = self.lakehouse.get_gold_records(table_name)
        if filter_key and filter_value:
            records = [r for r in records if str(r.get(filter_key, "")) == str(filter_value)]

        upstream = self.lineage.upstream_dependencies(f"gold.{table_name}")
        return {
            "query_status": "success",
            "table": table_name,
            "match_count": len(records),
            "results": records,
            "provenance_upstream": upstream,
            "audited_at": time.time(),
        }


class FabricCapacityMonitor:
    """Monitors Fabric Capacity Units (CUs) and spots runaway compute operations."""

    def __init__(self, capacity_sku: str = "F64", max_hourly_cu: float = 64.0) -> None:
        self.capacity_sku = capacity_sku
        self.max_hourly_cu = max_hourly_cu
        self._job_logs: list[dict[str, Any]] = []

    def record_job(
        self,
        job_id: str,
        job_type: str,  # "spark_notebook", "data_factory_pipeline", "direct_lake_query"
        cu_seconds: float,
        duration_seconds: float,
    ) -> dict[str, Any]:
        hourly_rate_estimate = (cu_seconds / 3600.0)
        is_runaway = hourly_rate_estimate > self.max_hourly_cu
        entry = {
            "job_id": job_id,
            "job_type": job_type,
            "cu_seconds": cu_seconds,
            "duration_seconds": duration_seconds,
            "burn_rate_cu": round(hourly_rate_estimate, 3),
            "is_runaway": is_runaway,
            "recorded_at": time.time(),
        }
        self._job_logs.append(entry)
        return entry

    def calculate_total_capacity_burn(self) -> float:
        return round(sum(j["cu_seconds"] for j in self._job_logs) / 3600.0, 3)

    def runaway_jobs(self) -> list[dict[str, Any]]:
        return [j for j in self._job_logs if j["is_runaway"]]
