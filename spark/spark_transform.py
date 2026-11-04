"""PySpark batch transform for daily revenue by region."""

from __future__ import annotations

import argparse
from pathlib import Path


def build_session(app_name: str = "dataplat-spark"):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def transform(spark, raw_dir: str | Path, out_dir: str | Path):
    from pyspark.sql import functions as F

    orders = spark.read.parquet(str(Path(raw_dir) / "orders.parquet"))
    daily = (
        orders.filter(F.col("status") == "paid")
        .withColumn("order_date", F.to_date("order_ts"))
        .groupBy("order_date", "region")
        .agg(
            F.count("*").alias("orders"),
            F.round(F.sum("amount_nzd"), 2).alias("revenue_nzd"),
        )
        .orderBy("order_date", "region")
    )
    daily.write.mode("overwrite").parquet(str(Path(out_dir) / "daily_revenue"))
    return daily


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", default="data/raw")
    parser.add_argument("--out", default="data/spark")
    args = parser.parse_args()
    spark = build_session()
    try:
        daily = transform(spark, args.raw, args.out)
        print(f"wrote {daily.count()} daily rows to {args.out}/daily_revenue")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
