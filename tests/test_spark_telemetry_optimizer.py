from dataplat.spark_telemetry_optimizer import SparkExecutionPlanOptimizer


def test_recommend_shuffle_partitions():
    optimizer = SparkExecutionPlanOptimizer()
    # 0 bytes -> min 2 partitions
    assert optimizer.recommend_shuffle_partitions(0) == 2
    # 256MB -> 2 partitions at 128MB
    assert optimizer.recommend_shuffle_partitions(256 * 1024 * 1024) == 2
    # 10GB -> ~80 partitions at 128MB
    assert optimizer.recommend_shuffle_partitions(10 * 1024 * 1024 * 1024) == 80


def test_detect_partition_skew():
    optimizer = SparkExecutionPlanOptimizer()
    # Balanced partitions
    balanced = [1000, 1050, 950, 1020]
    res_balanced = optimizer.detect_partition_skew(balanced)
    assert res_balanced["skew_detected"] is False
    assert len(res_balanced["skewed_indices"]) == 0

    # Highly skewed partition (partition 2 is 10x mean)
    skewed = [1000, 1000, 10000, 1000]
    res_skewed = optimizer.detect_partition_skew(skewed, skew_threshold_ratio=3.0)
    assert res_skewed["skew_detected"] is True
    assert 2 in res_skewed["skewed_indices"]


def test_analyze_physical_plan_text():
    optimizer = SparkExecutionPlanOptimizer()
    plan_with_cartesian = """
    == Physical Plan ==
    *(2) Project [id#0L, val#1]
    +- CartesianProduct
       :- *(1) Range (0, 1000, 1, 4)
       +- *(1) Range (0, 1000, 1, 4)
    """
    findings = optimizer.analyze_physical_plan_text(plan_with_cartesian)
    assert len(findings) >= 1
    assert any(f["issue"] == "CartesianProduct detected" for f in findings)


def test_generate_aqe_tuning_parameters():
    optimizer = SparkExecutionPlanOptimizer()
    params = optimizer.generate_aqe_tuning_parameters(enable_aqe=True, max_coalesce_partitions=300)
    assert params["spark.sql.adaptive.enabled"] == "true"
    assert params["spark.sql.adaptive.coalescePartitions.initialPartitionNum"] == "300"
    assert params["spark.sql.adaptive.skewJoin.enabled"] == "true"
