from datetime import datetime, timezone

from dataplat.warehouse import DuckDBWarehouse, SnowflakeWarehouse


class FakeCursor:
    def __init__(self, log):
        self.log = log
        self._rows = []

    def executemany(self, sql, rows):
        self.log.append((sql, list(rows)))

    def execute(self, sql):
        self.log.append((sql, None))

    def fetchall(self):
        return self._rows

    def close(self):
        return None


class FakeConnection:
    def __init__(self):
        self.log = []

    def cursor(self):
        return FakeCursor(self.log)


def row():
    return {
        "order_id": "O0000001",
        "customer_id": "C00001",
        "order_ts": datetime(2026, 11, 1, tzinfo=timezone.utc),
        "region": "Auckland",
        "amount_nzd": 120.50,
        "status": "paid",
    }


def test_snowflake_adapter_uses_merge():
    connection = FakeConnection()
    warehouse = SnowflakeWarehouse(connection)
    assert warehouse.upsert_orders([row()]) == 1
    sql, rows = connection.log[0]
    assert "MERGE INTO RAW.ORDERS" in sql
    assert rows[0][0] == "O0000001"


def test_duckdb_round_trip(tmp_path):
    warehouse = DuckDBWarehouse(tmp_path / "warehouse.duckdb")
    assert warehouse.upsert_orders([row()]) == 1
    result = warehouse.daily_revenue()
    assert result[0][1] == "Auckland"
    assert result[0][2] == 1
    assert float(result[0][3]) == 120.50
