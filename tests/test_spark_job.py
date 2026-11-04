import pytest

pytest.importorskip("pyspark")

from dataplat.generate import generate
from spark.spark_transform import build_session, transform

pytestmark = pytest.mark.spark


def test_spark_transform_writes_daily_revenue(tmp_path):
    raw = tmp_path / "raw"
    generate(raw, rows=200, seed=3)
    spark = build_session("dataplat-test")
    try:
        daily = transform(spark, raw, tmp_path / "out")
        assert daily.count() > 0
        assert {"order_date", "region", "orders", "revenue_nzd"}.issubset(set(daily.columns))
        assert (tmp_path / "out" / "daily_revenue").exists()
    finally:
        spark.stop()
