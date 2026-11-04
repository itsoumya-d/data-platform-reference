"""Warehouse adapters with a Snowflake-shaped upsert contract."""

from __future__ import annotations

import os
from contextlib import closing
from pathlib import Path
from typing import Any, Iterable, Protocol

MERGE_ORDERS = """
MERGE INTO RAW.ORDERS AS target
USING (
  SELECT %s AS ORDER_ID, %s AS CUSTOMER_ID, %s AS ORDER_TS,
         %s AS REGION, %s AS AMOUNT_NZD, %s AS STATUS
) AS source
ON target.ORDER_ID = source.ORDER_ID
WHEN MATCHED THEN UPDATE SET
  CUSTOMER_ID = source.CUSTOMER_ID,
  ORDER_TS = source.ORDER_TS,
  REGION = source.REGION,
  AMOUNT_NZD = source.AMOUNT_NZD,
  STATUS = source.STATUS
WHEN NOT MATCHED THEN INSERT
  (ORDER_ID, CUSTOMER_ID, ORDER_TS, REGION, AMOUNT_NZD, STATUS)
VALUES
  (source.ORDER_ID, source.CUSTOMER_ID, source.ORDER_TS,
   source.REGION, source.AMOUNT_NZD, source.STATUS)
""".strip()

DAILY_REVENUE_SQL = """
SELECT CAST(order_ts AS DATE) AS order_date,
       region,
       COUNT(*) AS orders,
       ROUND(SUM(amount_nzd), 2) AS revenue_nzd
FROM orders
WHERE status = 'paid'
GROUP BY 1, 2
ORDER BY 1, 2
""".strip()


class Cursor(Protocol):
    def executemany(self, sql: str, rows: Iterable[tuple[Any, ...]]) -> Any: ...
    def execute(self, sql: str) -> Any: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...


def order_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["order_id"],
        row["customer_id"],
        row["order_ts"],
        row["region"],
        row["amount_nzd"],
        row["status"],
    )


class SnowflakeWarehouse:
    """Thin Snowflake adapter; credentials come from the environment only."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    @classmethod
    def from_env(cls) -> "SnowflakeWarehouse":
        import snowflake.connector

        required = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise RuntimeError(f"missing Snowflake environment variables: {', '.join(missing)}")
        connection = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "DATAPLAT_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "DATAPLAT"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
        )
        return cls(connection)

    def upsert_orders(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with closing(self._connection.cursor()) as cursor:
            cursor.executemany(MERGE_ORDERS, [order_tuple(row) for row in rows])
        return len(rows)

    def daily_revenue(self) -> list[tuple[Any, ...]]:
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(DAILY_REVENUE_SQL)
            return list(cursor.fetchall())


class DuckDBWarehouse:
    """Local warehouse used by tests and the reference API."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)

    def _connect(self):
        import duckdb

        return duckdb.connect(self.path)

    def upsert_orders(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with closing(self._connect()) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                  order_id VARCHAR PRIMARY KEY,
                  customer_id VARCHAR,
                  order_ts TIMESTAMPTZ,
                  region VARCHAR,
                  amount_nzd DECIMAL(12, 2),
                  status VARCHAR
                )
                """
            )
            connection.executemany(
                "INSERT OR REPLACE INTO orders VALUES (?, ?, ?, ?, ?, ?)",
                [order_tuple(row) for row in rows],
            )
        return len(rows)

    def daily_revenue(self) -> list[tuple[Any, ...]]:
        with closing(self._connect()) as connection:
            return list(connection.execute(DAILY_REVENUE_SQL).fetchall())
