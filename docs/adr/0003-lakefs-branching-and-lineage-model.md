# ADR 0003: LakeFS Branching and Lineage Provenance

## Status
Accepted

## Context
Machine learning training experiments require deterministic reproducibility from raw multimodal captures to training datasets.

## Decision
We implement a LakeFS-style immutable commit tree recording exact content hashes for each sample version, tracking lineage across Bronze, Silver, and Gold transitions.

## Consequences
- **Positive:** Zero data drift; perfect model training reproducibility.
- **Negative:** Manifest storage scaling with billions of individual frames.
