"""Deterministic synthetic source data for the reference pipeline."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

REGIONS = ("Auckland", "Wellington", "Christchurch", "Hamilton", "Dunedin")
SEGMENTS = ("smb", "mid-market", "enterprise")
STATUSES = ("paid", "paid", "paid", "paid", "paid", "paid", "paid", "refunded", "failed")
EVENT_TYPES = ("created", "authorised", "captured", "refunded")
END = datetime(2026, 11, 4, tzinfo=timezone.utc)


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def generate(out_dir: str | Path, rows: int = 5000, seed: int = 42) -> dict[str, Path]:
    """Write orders, customers and events Parquet files and return their paths."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    rng = _rng(seed)

    customers = pd.DataFrame(
        {
            "customer_id": [f"C{i:05d}" for i in range(1, 501)],
            "segment": rng.choice(SEGMENTS, size=500, p=(0.6, 0.3, 0.1)),
            "country": rng.choice(("NZ", "AU", "US"), size=500, p=(0.8, 0.15, 0.05)),
            "signup_date": [
                (END - timedelta(days=int(d))).date().isoformat()
                for d in rng.integers(30, 900, size=500)
            ],
        }
    )

    customer_ids = customers["customer_id"].to_numpy()
    order_ts = [END - timedelta(minutes=int(m)) for m in rng.integers(0, 60 * 24 * 45, size=rows)]
    orders = pd.DataFrame(
        {
            "order_id": [f"O{i:07d}" for i in range(1, rows + 1)],
            "customer_id": rng.choice(customer_ids, size=rows),
            "order_ts": pd.to_datetime(order_ts, utc=True),
            "region": rng.choice(REGIONS, size=rows, p=(0.42, 0.2, 0.18, 0.1, 0.1)),
            "amount_nzd": np.round(rng.lognormal(mean=4.6, sigma=0.55, size=rows), 2),
            "status": rng.choice(STATUSES, size=rows),
        }
    )

    event_rows = []
    for order in orders.itertuples(index=False):
        count = int(rng.integers(1, 4))
        for offset, event_type in enumerate(EVENT_TYPES[:count]):
            event_rows.append(
                {
                    "event_id": f"E{len(event_rows) + 1:08d}",
                    "order_id": order.order_id,
                    "event_type": event_type,
                    "event_ts": order.order_ts + timedelta(seconds=30 * offset),
                }
            )
    events = pd.DataFrame(event_rows)

    paths = {
        "orders": out / "orders.parquet",
        "customers": out / "customers.parquet",
        "events": out / "events.parquet",
    }
    orders.to_parquet(paths["orders"], index=False)
    customers.to_parquet(paths["customers"], index=False)
    events.to_parquet(paths["events"], index=False)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data/raw")
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    paths = generate(args.out, rows=args.rows, seed=args.seed)
    for name, path in paths.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
