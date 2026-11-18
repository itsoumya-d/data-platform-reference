# Operations Runbook: Data Platform Reference

## Architecture & SLOs
- Ingestion Latency SLO: p95 < 200 ms for Bronze landing
- Query Latency SLO: p99 < 500 ms for lake metadata queries
- Service Availability: 99.9% uptime for `/health`

## Health Monitoring
Run smoke test:
```bash
./scripts/smoke.sh http://localhost:8000
```

## Failure Modes & Incidents
1. **Object storage throttling:** Adjust S3 prefix hash partitioning.
2. **Small-file bloat:** Trigger `plan_compaction` batch compaction jobs.
3. **Corrupted capture ingest:** Pipeline rejects zero-byte payloads and alerts on validation failure.

## Rollback Procedure
If a production deployment experiences degradation:
1. Revert deployment image tag to previous stable semver tag:
   ```bash
   docker pull ghcr.io/itsoumya-d/data-platform-reference:0.1.0
   ```
2. For lakehouse dataset rollbacks, use LakeFS commit snapshot revert:
   ```python
   lakefs.create_branch("hotfix-revert", source_commit_id="<last_known_good_commit>")
   ```
3. Restart container instances and execute `./scripts/smoke.sh`.
