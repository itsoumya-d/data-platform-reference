"""FastAPI surface over the local warehouse and insight service."""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException

from dataplat.ai_service import InsightService, load_notes
from dataplat.warehouse import DuckDBWarehouse


def create_app(db_path: str | Path | None = None, notes_path: str | Path | None = None) -> FastAPI:
    database = Path(db_path or os.environ.get("DATAPLAT_DB", "data/warehouse.duckdb"))
    notes_file = Path(notes_path or os.environ.get("DATAPLAT_NOTES", "data/raw/notes.parquet"))
    app = FastAPI(title="Data platform reference", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, object]:
        return {"status": "ok", "warehouse": database.exists(), "notes": notes_file.exists()}

    @app.get("/metrics/daily-revenue")
    def daily_revenue() -> list[dict[str, object]]:
        if not database.exists():
            return []
        rows = DuckDBWarehouse(database).daily_revenue()
        return [
            {"order_date": str(row[0]), "region": row[1], "orders": row[2], "revenue_nzd": float(row[3])}
            for row in rows
        ]

    @app.get("/customers/{customer_id}/insight")
    def customer_insight(customer_id: str, query: str = "payment risk refund delay") -> dict[str, object]:
        if not notes_file.exists():
            raise HTTPException(status_code=404, detail="notes dataset not found")
        service = InsightService(load_notes(notes_file))
        result = service.insight(customer_id, query=query)
        if not result["citations"]:
            raise HTTPException(status_code=404, detail="customer not found")
        return result

    return app


app = create_app()
