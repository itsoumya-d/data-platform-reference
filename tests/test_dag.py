import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag

pytestmark = pytest.mark.airflow


def test_dag_imports_and_has_expected_tasks():
    bag = DagBag("dags", include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags["dataplat_pipeline"]
    assert {"extract", "dbt_build", "spark_transform", "publish"}.issubset(set(dag.task_ids))
