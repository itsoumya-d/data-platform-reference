"""Bank of New Zealand (BNZ) Core Banking Data Lifecycle Engineering Engine.

Provides automated end-to-end lineage tracing, schema drift detection,
impact analysis across downstream Critical Data Elements (CDEs) and RBNZ regulatory
reports, source-to-target mapping generation, and root-cause reconciliation diagnostics.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple


class ChangeSeverity(enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL_BREAKING = "CRITICAL_BREAKING"


class LifecycleStage(enum.Enum):
    SOURCE_SYSTEM = "SOURCE_SYSTEM"
    INGESTION = "INGESTION"
    STAGING = "STAGING"
    TRANSFORMATION_DBT = "TRANSFORMATION_DBT"
    ANALYTICAL_MART = "ANALYTICAL_MART"
    REGULATORY_REPORTING = "REGULATORY_REPORTING"


@dataclass
class ColumnDefinition:
    name: str
    data_type: str
    is_nullable: bool = True
    is_critical_data_element: bool = False
    business_description: str = ""


@dataclass
class SchemaSnapshot:
    system_name: str
    entity_name: str
    version: str
    columns: Dict[str, ColumnDefinition]
    captured_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class SchemaChange:
    change_type: str  # ADDED, REMOVED, TYPE_CHANGED, NULLABILITY_CHANGED
    column_name: str
    old_value: Optional[str]
    new_value: Optional[str]
    severity: ChangeSeverity
    is_cde_affected: bool


@dataclass
class LineageNode:
    node_id: str
    stage: LifecycleStage
    entity_name: str
    columns: List[str]
    is_regulatory_target: bool = False
    regulatory_framework: Optional[str] = None  # e.g., "RBNZ_BS11", "BCBS_239"


@dataclass
class SourceToTargetMapping:
    source_system: str
    source_entity: str
    source_column: str
    target_entity: str
    target_column: str
    transformation_rule: str
    acceptance_criteria: str
    reconciliation_rule: str


class SchemaDriftDetector:
    """Detects structural and semantic schema changes between source system releases."""

    @staticmethod
    def compare_schemas(old: SchemaSnapshot, new: SchemaSnapshot) -> List[SchemaChange]:
        changes: List[SchemaChange] = []
        old_cols = old.columns
        new_cols = new.columns

        # Check removed columns
        for col_name, col_def in old_cols.items():
            if col_name not in new_cols:
                severity = ChangeSeverity.CRITICAL_BREAKING if col_def.is_critical_data_element else ChangeSeverity.HIGH
                changes.append(SchemaChange(
                    change_type="REMOVED",
                    column_name=col_name,
                    old_value=col_def.data_type,
                    new_value=None,
                    severity=severity,
                    is_cde_affected=col_def.is_critical_data_element
                ))

        # Check added and modified columns
        for col_name, new_def in new_cols.items():
            if col_name not in old_cols:
                changes.append(SchemaChange(
                    change_type="ADDED",
                    column_name=col_name,
                    old_value=None,
                    new_value=new_def.data_type,
                    severity=ChangeSeverity.LOW,
                    is_cde_affected=new_def.is_critical_data_element
                ))
            else:
                old_def = old_cols[col_name]
                # Type change check
                if old_def.data_type.lower() != new_def.data_type.lower():
                    severity = ChangeSeverity.CRITICAL_BREAKING if old_def.is_critical_data_element else ChangeSeverity.MEDIUM
                    changes.append(SchemaChange(
                        change_type="TYPE_CHANGED",
                        column_name=col_name,
                        old_value=old_def.data_type,
                        new_value=new_def.data_type,
                        severity=severity,
                        is_cde_affected=old_def.is_critical_data_element
                    ))
                # Nullability change check
                if old_def.is_nullable != new_def.is_nullable:
                    severity = ChangeSeverity.HIGH if (not old_def.is_nullable and new_def.is_nullable) else ChangeSeverity.LOW
                    changes.append(SchemaChange(
                        change_type="NULLABILITY_CHANGED",
                        column_name=col_name,
                        old_value=str(old_def.is_nullable),
                        new_value=str(new_def.is_nullable),
                        severity=severity,
                        is_cde_affected=old_def.is_critical_data_element
                    ))

        return changes


class DataLineageGraph:
    """Directed graph representing end-to-end data flows from core banking sources to regulatory marts."""

    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: Dict[str, Set[str]] = {}  # source_id -> set of downstream_ids

    def add_node(self, node: LineageNode) -> None:
        self.nodes[node.node_id] = node
        if node.node_id not in self.edges:
            self.edges[node.node_id] = set()

    def add_edge(self, from_id: str, to_id: str) -> None:
        if from_id not in self.nodes or to_id not in self.nodes:
            raise ValueError(f"Both nodes {from_id} and {to_id} must exist before creating edge")
        self.edges[from_id].add(to_id)

    def trace_downstream(self, start_node_id: str) -> List[LineageNode]:
        """Performs breadth-first traversal to discover all affected downstream consumers."""
        if start_node_id not in self.nodes:
            return []

        visited: Set[str] = set()
        queue: List[str] = [start_node_id]
        downstream: List[LineageNode] = []

        while queue:
            curr = queue.pop(0)
            for child in self.edges.get(curr, set()):
                if child not in visited:
                    visited.add(child)
                    queue.append(child)
                    downstream.append(self.nodes[child])

        return downstream

    def get_impacted_regulatory_reports(self, start_node_id: str) -> List[LineageNode]:
        """Identifies regulatory reporting artifacts (e.g. RBNZ BS11, APRA/BCBS 239) impacted by an upstream change."""
        all_downstream = self.trace_downstream(start_node_id)
        return [n for n in all_downstream if n.is_regulatory_target]


class ReconciliationRootCauseAnalyzer:
    """Investigates financial ledger imbalances and isolates the lifecycle failure stage."""

    @staticmethod
    def diagnose_imbalance(
        source_balance: float,
        staged_balance: float,
        mart_balance: float,
        report_balance: float,
        tolerance: float = 0.01
    ) -> Dict[str, Any]:
        discrepancy = round(source_balance - report_balance, 4)
        if abs(discrepancy) <= tolerance:
            return {
                "status": "BALANCED",
                "discrepancy": 0.0,
                "failing_stage": None,
                "root_cause": "Balances reconcile within tolerance across all lifecycle stages.",
                "remedy": "None required."
            }

        # Check Ingestion/Staging drop
        if abs(source_balance - staged_balance) > tolerance:
            stage_diff = round(source_balance - staged_balance, 4)
            return {
                "status": "IMBALANCED",
                "discrepancy": discrepancy,
                "failing_stage": LifecycleStage.INGESTION.value,
                "root_cause": f"Source-to-staging ingestion truncated {stage_diff:,.2f} in transactions. Potential batch filter or dropped record anomaly.",
                "remedy": "Replay ingestion CDC pipeline for partition with source row count validation."
            }

        # Check Transformation/dbt drop
        if abs(staged_balance - mart_balance) > tolerance:
            stage_diff = round(staged_balance - mart_balance, 4)
            return {
                "status": "IMBALANCED",
                "discrepancy": discrepancy,
                "failing_stage": LifecycleStage.TRANSFORMATION_DBT.value,
                "root_cause": f"Transformation mart dropped {stage_diff:,.2f}. Inner join filtering or unmapped lending facility code.",
                "remedy": "Inspect dbt model join predicates and audit unmapped facility reference lookups."
            }

        # Check Final Aggregation / Reporting drop
        if abs(mart_balance - report_balance) > tolerance:
            stage_diff = round(mart_balance - report_balance, 4)
            return {
                "status": "IMBALANCED",
                "discrepancy": discrepancy,
                "failing_stage": LifecycleStage.REGULATORY_REPORTING.value,
                "root_cause": f"Reporting layer aggregation mismatch of {stage_diff:,.2f}. Group by filter or rounding truncation.",
                "remedy": "Verify reporting SQL sum precision and check date window cut-off parameters."
            }

        return {
            "status": "UNKNOWN_MISMATCH",
            "discrepancy": discrepancy,
            "failing_stage": None,
            "root_cause": "Unclassified reconciliation anomaly.",
            "remedy": "Conduct detailed transaction-level hash diff across partitions."
        }


class BNZDataLifecycleManager:
    """Orchestrates end-to-end data lifecycle governance for BNZ Core Banking."""

    def __init__(self, lineage_graph: DataLineageGraph):
        self.lineage_graph = lineage_graph
        self.mappings: List[SourceToTargetMapping] = []

    def register_mapping(self, mapping: SourceToTargetMapping) -> None:
        self.mappings.append(mapping)

    def evaluate_release_impact(
        self,
        old_schema: SchemaSnapshot,
        new_schema: SchemaSnapshot,
        source_node_id: str
    ) -> Dict[str, Any]:
        """Evaluates full release impact: schema drift, affected lineage, and regulatory exposures."""
        changes = SchemaDriftDetector.compare_schemas(old_schema, new_schema)
        downstream_nodes = self.lineage_graph.trace_downstream(source_node_id)
        regulatory_reports = self.lineage_graph.get_impacted_regulatory_reports(source_node_id)

        has_critical = any(c.severity == ChangeSeverity.CRITICAL_BREAKING for c in changes)
        cde_impacted = any(c.is_cde_affected for c in changes)

        return {
            "source_system": old_schema.system_name,
            "entity": old_schema.entity_name,
            "total_changes": len(changes),
            "has_critical_breaking_change": has_critical,
            "cde_impacted": cde_impacted,
            "changes": [
                {
                    "type": c.change_type,
                    "column": c.column_name,
                    "severity": c.severity.value,
                    "is_cde": c.is_cde_affected
                }
                for c in changes
            ],
            "impacted_downstream_count": len(downstream_nodes),
            "impacted_regulatory_frameworks": list({
                n.regulatory_framework for n in regulatory_reports if n.regulatory_framework
            })
        }
