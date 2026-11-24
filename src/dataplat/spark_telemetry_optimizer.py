"""Spark execution plan analysis and telemetry optimization utilities."""

import math
from typing import Any


class SparkExecutionPlanOptimizer:
    """Provides internal tuning, partition sizing, and skew detection for Spark queries."""

    DEFAULT_TARGET_PARTITION_MB = 128

    @staticmethod
    def recommend_shuffle_partitions(
        total_input_bytes: int, target_partition_mb: int = DEFAULT_TARGET_PARTITION_MB
    ) -> int:
        """Calculate optimal spark.sql.shuffle.partitions based on input data volume."""
        if total_input_bytes <= 0:
            return 2
        target_bytes = target_partition_mb * 1024 * 1024
        partitions = math.ceil(total_input_bytes / target_bytes)
        # Clamp between 2 and 4000 for standard cluster configurations
        return max(2, min(partitions, 4000))

    @staticmethod
    def detect_partition_skew(
        partition_sizes_bytes: list[int], skew_threshold_ratio: float = 3.0
    ) -> dict[str, Any]:
        """Identify data skew across shuffle partitions exceeding the threshold ratio."""
        if not partition_sizes_bytes:
            return {"skew_detected": False, "skew_ratio": 1.0, "skewed_indices": []}

        mean_size = sum(partition_sizes_bytes) / len(partition_sizes_bytes)
        max_size = max(partition_sizes_bytes)
        skew_ratio = (max_size / mean_size) if mean_size > 0 else 1.0

        skewed_indices = [
            idx
            for idx, size in enumerate(partition_sizes_bytes)
            if mean_size > 0 and (size / mean_size) >= skew_threshold_ratio
        ]

        return {
            "skew_detected": skew_ratio >= skew_threshold_ratio,
            "skew_ratio": round(skew_ratio, 2),
            "skewed_indices": skewed_indices,
            "mean_partition_bytes": int(mean_size),
            "max_partition_bytes": max_size,
        }

    @staticmethod
    def analyze_physical_plan_text(plan_text: str) -> list[dict[str, str]]:
        """Scan physical execution plan string for anti-patterns and bottlenecks."""
        findings = []
        low = plan_text.lower()

        if "cartesianproduct" in low or "cartesian product" in low:
            findings.append({
                "severity": "CRITICAL",
                "issue": "CartesianProduct detected",
                "recommendation": "Replace cross join with broadcast join or keyed hash join",
            })
        if "broadcastnestedloopjoin" in low:
            findings.append({
                "severity": "HIGH",
                "issue": "BroadcastNestedLoopJoin detected without equality predicate",
                "recommendation": "Add equijoin join conditions to enable HashJoin",
            })
        if "repartition" in low and "coalesce" not in low:
            findings.append({
                "severity": "INFO",
                "issue": "Explicit repartition triggers full network shuffle",
                "recommendation": "Evaluate coalesce() if reducing partition counts",
            })

        return findings

    @staticmethod
    def generate_aqe_tuning_parameters(
        enable_aqe: bool = True, max_coalesce_partitions: int = 200
    ) -> dict[str, Any]:
        """Generate Adaptive Query Execution (AQE) tuning configurations."""
        return {
            "spark.sql.adaptive.enabled": str(enable_aqe).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.initialPartitionNum": str(max_coalesce_partitions),
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "3",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "67108864",  # 64MB
        }
