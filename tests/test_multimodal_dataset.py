"""Unit tests for multimodal training dataset sharder and validator."""

import pytest

from dataplat.multimodal_dataset import (
    DatasetValidationError,
    MultimodalDatasetSharder,
    MultimodalSample,
)


def test_valid_sample_registration() -> None:
    sample = MultimodalSample(
        sample_id="img_001",
        modality="image",
        payload=b"\x89PNG\r\n\x1a\nfakeimagebytes",
        metadata={"width": 512, "height": 512, "channels": 3},
        license_tier="open",
        pii_cleared=True,
    )
    sharder = MultimodalDatasetSharder(max_samples_per_shard=10, target_tier="open")
    shard_id = sharder.add_sample(sample)
    assert shard_id == 0

    manifests = sharder.finalize()
    assert len(manifests) == 1
    assert manifests[0].sample_count == 1
    assert manifests[0].license_tier == "open"
    assert "img_001" in manifests[0].sample_ids


def test_checksum_mismatch_raises_error() -> None:
    with pytest.raises(DatasetValidationError, match="Checksum mismatch"):
        MultimodalSample(
            sample_id="audio_001",
            modality="audio",
            payload=b"fakeaudiobytes",
            sha256_checksum="deadbeef00000000000000000000000000000000000000000000000000000000",
        )


def test_unsupported_modality_rejected() -> None:
    sample = MultimodalSample(
        sample_id="haptic_001",
        modality="haptic_telemetry",
        payload=b"sensordata",
        license_tier="open",
    )
    sharder = MultimodalDatasetSharder(target_tier="open")
    with pytest.raises(DatasetValidationError, match="Unsupported modality"):
        sharder.add_sample(sample)


def test_license_tier_separation() -> None:
    restricted_sample = MultimodalSample(
        sample_id="vid_001",
        modality="video",
        payload=b"restrictedvideocontent",
        license_tier="restricted",
    )
    sharder = MultimodalDatasetSharder(target_tier="open")
    with pytest.raises(DatasetValidationError, match="License tier violation"):
        sharder.add_sample(restricted_sample)


def test_pii_clearance_enforcement() -> None:
    uncleared_sample = MultimodalSample(
        sample_id="txt_001",
        modality="text",
        payload=b"user document with PII",
        license_tier="open",
        pii_cleared=False,
    )
    sharder = MultimodalDatasetSharder(target_tier="open", enforce_pii_clearance=True)
    with pytest.raises(DatasetValidationError, match="PII clearance violation"):
        sharder.add_sample(uncleared_sample)


def test_shard_rollover_and_manifest_sealing() -> None:
    sharder = MultimodalDatasetSharder(max_samples_per_shard=2, target_tier="open")
    for i in range(5):
        s = MultimodalSample(
            sample_id=f"sample_{i}",
            modality="image",
            payload=f"content_{i}".encode(),
            license_tier="open",
        )
        sharder.add_sample(s)

    manifests = sharder.finalize()
    assert len(manifests) == 3
    assert manifests[0].sample_count == 2
    assert manifests[1].sample_count == 2
    assert manifests[2].sample_count == 1
    assert manifests[0].content_hash != manifests[1].content_hash


def test_shard_retrieval_and_bounds() -> None:
    sharder = MultimodalDatasetSharder(max_samples_per_shard=5, target_tier="open")
    s = MultimodalSample(sample_id="s1", modality="audio", payload=b"wav", license_tier="open")
    sharder.add_sample(s)
    samples = sharder.get_shard_samples(0)
    assert len(samples) == 1
    assert samples[0].sample_id == "s1"

    with pytest.raises(IndexError):
        sharder.get_shard_samples(99)
