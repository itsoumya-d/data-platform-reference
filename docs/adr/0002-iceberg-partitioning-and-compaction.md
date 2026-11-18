# ADR 0002: Iceberg Partitioning and Compaction Strategy

## Status
Accepted

## Context
High-volume sensor ingestion creates thousands of small files under minute-level partitioning, creating metadata explosion and high latency in object storage.

## Decision
We enforce hierarchical partitioning (`sensor_type / timestamp_epoch`) paired with automated compaction planning grouping files into target 128 MB blocks.

## Consequences
- **Positive:** Minimizes S3 list operations and small-file request throttling.
- **Negative:** Requires background compaction batch jobs.
