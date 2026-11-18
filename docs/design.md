# Design Document: Multimodal Spatial Data Lakehouse Architecture

## 1. Context and Scope
Embodied AI, 3D robotics perception, and world models require petabyte-scale 4D multimodal spatial sensor data (egocentric video, LiDAR point clouds, calibrated depth streams, and IMU telemetry). Traditional tabular data warehouses and object storage patterns suffer from small-file overheads, lacking immutable dataset branching and zero-bubble streaming GPU loaders.
This design introduces an integrated lakehouse engine built on open table standards (Apache Iceberg metadata layout), dataset version control (LakeFS branching semantics), and streaming GPU shard dispatchers (WebDataset/Mosaic format).

## 2. Goals and Non-Goals
### Goals
- Provide content-addressed SHA-256 deduplication and atomic Bronze -> Silver -> Gold promotion.
- Maintain immutable, reproducible dataset branching and end-to-end lineage tracking.
- Mitigate small-file object storage penalties via adaptive compaction batch planning.
- Stream training samples with chunked prefetching to maximize GPU utilization.

### Non-Goals
- Real-time in-sensor hardware firmware compilation (handled upstream at device capture edge).
- Proprietary vendor-locked storage formats.

## 3. Architecture & Key Decisions
- **Storage Tiering (Bronze -> Silver -> Gold):**
  - **Bronze:** Immutable raw captures partitioned by sensor type and epoch seconds.
  - **Silver:** Sensor-calibrated and temporally synchronized metadata.
  - **Gold:** Feature-embedded, spatially labeled datasets ready for distributed training clusters.
- **Open Table Layout:** Apache Iceberg partition specs with hierarchical metadata snapshots.
- **Dataset Versioning:** LakeFS-style branching model (`main`, experiment branches) with parent commit graphs.
- **Distributed Training Ingestion:** Sharded WebDataset/MosaicStreaming export with chunk prefetching.

## 4. Alternatives Considered
- **Direct Parquet on S3 without Table Catalogs:** Rejected due to schema evolution overhead and small-file explosion during multi-camera capture ingestion.
- **Delta Lake with proprietary catalogs:** Rejected in favor of open Iceberg partition specs allowing multi-engine query interoperability (Trino, DuckDB, Spark).

## 5. Cross-Cutting Concerns
- **Security & Integrity:** Cryptographic SHA-256 validation of all sensor payloads at ingest boundaries.
- **Observability:** Granular snapshot logs and compaction batch metrics.
- **Cost Efficiency:** Automated compaction planning grouping sub-target files into single object storage blocks.
