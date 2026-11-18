import pytest
from dataplat.spatial_lakehouse import CaptureRecord, MultimodalIngestPipeline, LakeFSBranchManager


def test_corrupted_payload_graceful_degradation(tmp_path):
    pipeline = MultimodalIngestPipeline(tmp_path)
    # Empty corrupted payload
    record = CaptureRecord(
        capture_id="corrupt_01",
        sensor_type="lidar",
        timestamp_ns=1700000000,
        data_payload=b"",
    )
    status = pipeline.ingest_to_bronze(record)
    assert status.validation_passed is False
    assert "Empty payload" in (status.error_message or "")


def test_nonexistent_branch_error_handling():
    mgr = LakeFSBranchManager()
    with pytest.raises(ValueError, match="not found"):
        mgr.commit("nonexistent_branch", "test message", {})
