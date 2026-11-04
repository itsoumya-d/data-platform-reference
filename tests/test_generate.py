import pandas as pd

from dataplat.generate import generate


def test_generate_is_deterministic(tmp_path):
    first = generate(tmp_path / "one", rows=300, seed=7)
    second = generate(tmp_path / "two", rows=300, seed=7)
    orders_one = pd.read_parquet(first["orders"])
    orders_two = pd.read_parquet(second["orders"])
    assert len(orders_one) == 300
    assert len(pd.read_parquet(first["customers"])) == 500
    assert {"order_id", "customer_id", "order_ts", "region", "amount_nzd", "status"}.issubset(orders_one.columns)
    assert orders_one["amount_nzd"].tolist() == orders_two["amount_nzd"].tolist()
    assert orders_one["status"].isin({"paid", "refunded", "failed"}).all()
