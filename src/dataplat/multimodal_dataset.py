"""Multimodal training dataset sharding, schema validation, and PII tiering engine."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, ClassVar


class DatasetValidationError(ValueError):
    """Raised when a multimodal sample violates schema, checksum, or tiering rules."""


@dataclass
class MultimodalSample:
    sample_id: str
    modality: str  # "image", "audio", "video", "text"
    payload: bytes
    metadata: dict[str, Any] = field(default_factory=dict)
    license_tier: str = "open"  # "open", "restricted"
    pii_cleared: bool = True
    sha256_checksum: str | None = None

    def __post_init__(self) -> None:
        computed = hashlib.sha256(self.payload).hexdigest()
        if self.sha256_checksum and self.sha256_checksum != computed:
            raise DatasetValidationError(
                f"Checksum mismatch for sample {self.sample_id}: expected {self.sha256_checksum}, got {computed}"
            )
        self.sha256_checksum = computed


@dataclass
class ShardManifest:
    shard_id: int
    sample_count: int
    total_bytes: int
    sample_ids: list[str]
    license_tier: str
    content_hash: str


class MultimodalDatasetSharder:
    """Partitions and validates multimodal training samples into deterministic shards."""

    SUPPORTED_MODALITIES: ClassVar[set[str]] = {"image", "audio", "video", "text"}
    VALID_TIERS: ClassVar[set[str]] = {"open", "restricted"}

    def __init__(
        self,
        max_samples_per_shard: int = 100,
        target_tier: str = "open",
        enforce_pii_clearance: bool = True,
    ) -> None:
        if target_tier not in self.VALID_TIERS:
            raise ValueError(f"Invalid target tier: {target_tier}")
        if max_samples_per_shard <= 0:
            raise ValueError("max_samples_per_shard must be positive")

        self.max_samples_per_shard = max_samples_per_shard
        self.target_tier = target_tier
        self.enforce_pii_clearance = enforce_pii_clearance

        self._shards: list[list[MultimodalSample]] = [[]]
        self._manifests: list[ShardManifest] = []

    def add_sample(self, sample: MultimodalSample) -> int:
        """Validate sample and assign it to the current shard. Returns assigned shard_id."""
        if sample.modality not in self.SUPPORTED_MODALITIES:
            raise DatasetValidationError(f"Unsupported modality: {sample.modality}")

        if sample.license_tier != self.target_tier:
            raise DatasetValidationError(
                f"License tier violation: sample has tier '{sample.license_tier}', shard requires '{self.target_tier}'"
            )

        if self.enforce_pii_clearance and not sample.pii_cleared:
            raise DatasetValidationError(
                f"PII clearance violation: sample {sample.sample_id} contains uncleared PII"
            )

        current_shard_idx = len(self._shards) - 1
        if len(self._shards[current_shard_idx]) >= self.max_samples_per_shard:
            self._seal_current_shard()
            self._shards.append([])
            current_shard_idx = len(self._shards) - 1

        self._shards[current_shard_idx].append(sample)
        return current_shard_idx

    def _seal_current_shard(self) -> None:
        idx = len(self._shards) - 1
        samples = self._shards[idx]
        if not samples:
            return

        total_bytes = sum(len(s.payload) for s in samples)
        sample_ids = [s.sample_id for s in samples]

        hasher = hashlib.sha256()
        for s in samples:
            hasher.update((s.sha256_checksum or "").encode("ascii"))

        manifest = ShardManifest(
            shard_id=idx,
            sample_count=len(samples),
            total_bytes=total_bytes,
            sample_ids=sample_ids,
            license_tier=self.target_tier,
            content_hash=hasher.hexdigest(),
        )
        self._manifests.append(manifest)

    def finalize(self) -> list[ShardManifest]:
        """Seal remaining samples and export list of shard manifests."""
        current_shard = self._shards[-1]
        if current_shard and (not self._manifests or self._manifests[-1].shard_id != len(self._shards) - 1):
            self._seal_current_shard()
        return list(self._manifests)

    def get_shard_samples(self, shard_id: int) -> list[MultimodalSample]:
        """Retrieve samples in a specific shard."""
        if shard_id < 0 or shard_id >= len(self._shards):
            raise IndexError(f"Shard index {shard_id} out of bounds")
        return list(self._shards[shard_id])
