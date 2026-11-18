# ADR 0001: Multimodal Spatial Lakehouse Architecture with Iceberg and LakeFS

## Status
Accepted

## Context
High-fidelity spatial AI data pipelines handle heterogenous, high-volume sensor streams. Storing these raw streams naively in object storage leads to partitioned data bloat, small-file performance degradation, and irreproducible training datasets.

## Decision
We implement a tripartite architecture:
1. Content-addressed ingest with automated Bronze -> Silver -> Gold stage gates.
2. Apache Iceberg open table metadata format with snapshot isolation and scheduled compaction planning.
3. LakeFS-compatible branching and immutable commit lineage to guarantee experiment reproducibility.
4. WebDataset/MosaicStreaming sharding for GPU compute clusters.

## Alternatives Considered
1. *Raw Flat S3 Buckets:* High risk of duplicate uploads and untracked schema drift.
2. *Proprietary Lakehouse Formats:* Limits query flexibility across diverse open-source analytical engines.

## Consequences
- **Positive:** Guaranteed data provenance from field capture to GPU training; eliminated small-file overhead.
- **Negative:** Additional metadata maintenance and snapshot tracking overhead.
