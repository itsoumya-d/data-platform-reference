import pathlib
import tempfile

from dataplat.spatial_lakehouse import (
    CaptureRecord,
    GPUTrainingStreamLoader,
    IcebergTableManager,
    LakeFSBranchManager,
    MultimodalIngestPipeline,
    SpatialLakehouseEngine,
)


def test_capture_record_hash_consistency():
    c1 = CaptureRecord(
        capture_id="cap_001",
        sensor_type="egocentric_video",
        timestamp_ns=1700000000000000000,
        data_payload=b"video_frame_bytes_12345",
    )
    c2 = CaptureRecord(
        capture_id="cap_001",
        sensor_type="egocentric_video",
        timestamp_ns=1700000000000000000,
        data_payload=b"video_frame_bytes_12345",
    )
    c3 = CaptureRecord(
        capture_id="cap_002",
        sensor_type="egocentric_video",
        timestamp_ns=1700000000000000001,
        data_payload=b"video_frame_bytes_12345",
    )
    assert c1.content_hash == c2.content_hash
    assert c1.content_hash != c3.content_hash


def test_bronze_silver_gold_promotion():
    with tempfile.TemporaryDirectory() as td:
        pipe = MultimodalIngestPipeline(td)
        rec = CaptureRecord(
            capture_id="cap_100",
            sensor_type="lidar_pointcloud",
            timestamp_ns=1700000000000000000,
            data_payload=b"point_cloud_data_xyz_intensity",
        )

        # 1. Bronze
        b_res = pipe.ingest_to_bronze(rec)
        assert b_res.stage == "bronze"
        assert b_res.validation_passed is True
        assert b_res.deduplicated is False
        assert pathlib.Path(b_res.file_path).exists()

        # 2. Duplicate ingestion check
        b_dup = pipe.ingest_to_bronze(rec)
        assert b_dup.deduplicated is True

        # 3. Silver
        s_res = pipe.promote_to_silver(b_res, calibrated_metadata={"intrinsics": [1.0, 0.0, 0.0]})
        assert s_res.stage == "silver"
        assert pathlib.Path(s_res.file_path).exists()

        # 4. Gold
        g_res = pipe.promote_to_gold(
            s_res,
            feature_embeddings=[0.1, 0.2, 0.3],
            labels={"annotated": True, "num_objects": 4},
        )
        assert g_res.stage == "gold"
        assert pathlib.Path(g_res.file_path).exists()


def test_iceberg_metadata_and_compaction():
    with tempfile.TemporaryDirectory() as td:
        mgr = IcebergTableManager("multimodal_spatial", td)
        snap1 = mgr.create_snapshot(
            added_files=["s3://lake/silver/part1.parquet"],
            partition_counts={"egocentric_video": 1500},
        )
        assert snap1.snapshot_id is not None
        assert snap1.total_records == 1500
        assert snap1.parent_snapshot_id is None

        snap2 = mgr.create_snapshot(
            added_files=["s3://lake/silver/part2.parquet"],
            partition_counts={"lidar_pointcloud": 2000},
        )
        assert snap2.total_records == 3500
        assert snap2.parent_snapshot_id == snap1.snapshot_id

        # Compaction planning: target 100MB
        files = {
            "file_a.parquet": 20 * 1024 * 1024,
            "file_b.parquet": 30 * 1024 * 1024,
            "file_c.parquet": 40 * 1024 * 1024,
            "file_d.parquet": 150 * 1024 * 1024,  # already larger than target
        }
        batches = mgr.plan_compaction(files, target_size_bytes=100 * 1024 * 1024)
        assert len(batches) == 1
        assert set(batches[0]) == {"file_a.parquet", "file_b.parquet", "file_c.parquet"}


def test_lakefs_branching_and_lineage():
    mgr = LakeFSBranchManager()
    assert "main" in mgr.branches

    # Create feature branch
    dev_branch = mgr.create_branch("exp/depth-calibration", source_branch="main")
    assert dev_branch.name == "exp/depth-calibration"

    # Commit on branch
    c1 = mgr.commit(
        "exp/depth-calibration",
        "Add calibrated sensor batch A",
        {"cap_01": "hash_abc", "cap_02": "hash_def"},
    )
    assert c1.branch == "exp/depth-calibration"
    assert len(c1.manifest) == 2

    c2 = mgr.commit(
        "exp/depth-calibration",
        "Add calibrated sensor batch B",
        {"cap_03": "hash_ghi"},
    )
    assert len(c2.manifest) == 3

    # Check lineage
    lineage = mgr.get_lineage(c2.commit_id)
    assert len(lineage) == 3  # c2 -> c1 -> initial
    assert lineage[0].commit_id == c2.commit_id
    assert lineage[1].commit_id == c1.commit_id


def test_gpu_training_loader_and_prefetch():
    with tempfile.TemporaryDirectory() as td:
        loader = GPUTrainingStreamLoader(td, max_samples_per_shard=10)
        samples = [{"capture_id": f"cap_{i}", "embedding": [float(i)]} for i in range(25)]

        shards = loader.export_shards(samples, format_type="webdataset")
        assert len(shards) == 3  # 10 + 10 + 5
        assert shards[0].sample_count == 10
        assert shards[2].sample_count == 5

        # Prefetch generator
        batches = list(loader.prefetch_generator([s.shard_id for s in shards], batch_size=8))
        assert len(batches) == 4  # 8, 8, 8, 1
        assert sum(len(b) for b in batches) == 25


def test_spatial_lakehouse_engine_e2e():
    with tempfile.TemporaryDirectory() as td:
        engine = SpatialLakehouseEngine(td)
        records = [
            CaptureRecord(
                capture_id=f"cap_e2e_{i}",
                sensor_type="depth_map" if i % 2 == 0 else "imu_telemetry",
                timestamp_ns=1700000000000000000 + i * 1000,
                data_payload=f"multimodal_payload_{i}".encode(),
            )
            for i in range(6)
        ]

        res = engine.run_e2e_capture_flow(records, branch_name="main")
        assert res["ingested_count"] == 6
        assert res["iceberg_snapshot_id"] is not None
        assert res["lakefs_commit_id"] is not None
        assert res["shards_created"] >= 1
