from datetime import datetime, timezone

import pandas as pd
from fastapi.testclient import TestClient

from dataplat.api import create_app
from dataplat.warehouse import DuckDBWarehouse


def test_api_health_metrics_and_insight(tmp_path):
    database = tmp_path / "warehouse.duckdb"
    DuckDBWarehouse(database).upsert_orders(
        [
            {
                "order_id": "O0000001",
                "customer_id": "C00001",
                "order_ts": datetime(2026, 11, 1, tzinfo=timezone.utc),
                "region": "Auckland",
                "amount_nzd": 120.50,
                "status": "paid",
            }
        ]
    )
    notes_path = tmp_path / "notes.parquet"
    pd.DataFrame(
        [{"note_id": "N1", "customer_id": "C00001", "text": "A refund delay caused a payment risk review."}]
    ).to_parquet(notes_path, index=False)

    client = TestClient(create_app(database, notes_path))
    assert client.get("/health").json()["status"] == "ok"
    metrics = client.get("/metrics/daily-revenue").json()
    assert metrics[0]["region"] == "Auckland"
    insight = client.get("/customers/C00001/insight", params={"query": "refund delay"}).json()
    assert insight["citations"] == ["N1"]
    assert client.get("/customers/C99999/insight").status_code == 404
