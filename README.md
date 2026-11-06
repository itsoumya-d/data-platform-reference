# data-platform-reference

[![ci](https://github.com/itsoumya-d/data-platform-reference/actions/workflows/ci.yml/badge.svg)](https://github.com/itsoumya-d/data-platform-reference/actions/workflows/ci.yml)

A small, testable reference data platform built around the stack used by
New Zealand data-engineering teams: Python, SQL, dbt, Airflow, Spark, Kafka,
Snowflake, Terraform, Azure and FastAPI.

This is a **reference implementation**, not a claim of production experience.
The Snowflake and Kafka adapters are unit-tested; the local dbt target runs on
DuckDB; Spark runs in local mode; no live Snowflake account, Kafka broker or
Azure subscription is used by CI.

## What it does

1. Generates deterministic synthetic orders, customers and events.
2. Builds staging and mart models with dbt (DuckDB locally, Snowflake target
   configured).
3. Orchestrates extract -> dbt -> Spark -> publish with an Airflow DAG.
4. Recomputes daily revenue by region with PySpark.
5. Publishes JSON event envelopes through a Kafka-shaped bus.
6. Serves warehouse metrics and retrieval-backed customer insights through
   FastAPI.
7. Provisions an Azure storage account and private raw container with
   Terraform (validated, not applied).

## Stack

| Layer | Implementation | Verification |
|---|---|---|
| Ingestion | Python + pandas + Parquet | pytest |
| Warehouse | DuckDB execution; Snowflake MERGE adapter | unit tests + local round trip |
| Transform | dbt models and schema tests | `dbt build` on DuckDB |
| Orchestration | Airflow `@dag` / `@task` | DAG import test |
| Batch | PySpark local mode | pytest with Java 17 |
| Streaming | Kafka producer + in-memory bus | unit tests |
| API | FastAPI + TestClient | pytest |
| AI service | TF-IDF retrieval + extractive, cited summaries | pytest |
| Infrastructure | Terraform azurerm module | `terraform validate` |

## Layout

```
src/dataplat/        generate, warehouse, kafka_bus, ai_service, api
dbt_project/         dbt models, macros, DuckDB + Snowflake profiles
dags/                Airflow pipeline
spark/               PySpark daily-revenue job
terraform/           Azure storage + private raw container
tests/               unit, API, dbt-adjacent, DAG and Spark tests
```

## Run locally

```bash
uv venv --python 3.12
source .venv/bin/activate
pip install -e ".[dev,dbt]"
python -m dataplat.generate --out data/raw --rows 2000
cd dbt_project
dbt build --profiles-dir . --target duckdb
cd ..
pytest -m "not spark and not airflow and not integration"
uvicorn dataplat.api:app --reload
```

Optional extras:

```bash
pip install -e ".[spark]"     # PySpark local job
pip install -e ".[kafka]"     # confluent-kafka producer
pip install -e ".[snowflake]" # snowflake-connector-python
pip install -e ".[airflow]"   # Airflow DAG parsing
```


## Heartland banking Data Vault extension

This repo also carries a Heartland-shaped NZ/AU banking slice:

- `adf/pipeline_ingest_core_banking.json` — parameterised Azure Data Factory
  copy pipeline for core-banking accounts and transactions.
- `dbt_project/models/raw_vault/` — Data Vault 2.0 hubs, link and satellites
  for customers and accounts, with not-null and uniqueness data tests.
- `dbt_project/models/marts/fct_daily_balance.sql`,
  `dim_account.sql` and `mart_deposit_lending.sql` — finance and regulatory
  reporting marts.
- `semantic/heartland_metrics.json` — AtScale-style semantic metrics for
  deposits, lending, daily flow, CET1 and liquidity coverage.
- `src/dataplat/heartland.py` — pure hash keys, daily net-flow aggregation,
  deposit/lending split and regulatory ratios.

The banking slice is validated by 26 pytest tests and `dbt build` (45 nodes:
10 table models, 5 views and 30 data tests) on DuckDB. Snowflake, ADF and
AtScale remain adapters/contracts, not live external services.

## Boundaries

- `SnowflakeWarehouse.from_env()` reads credentials only from environment
  variables. CI never connects to Snowflake; the MERGE adapter is tested with a
  fake cursor.
- `KafkaBus` uses `confluent-kafka` lazily. CI tests the envelope and the
  in-memory bus, not a live broker.
- dbt uses DuckDB in CI. The Snowflake profile is present for a real target but
  is not executed.
- Terraform is formatted and validated; it is not applied.

## License

MIT
