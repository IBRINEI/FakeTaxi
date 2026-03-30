import pytest
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(dag_folder='/opt/airflow/dags', include_examples=False)


def test_dag_loaded_without_errors(dag_bag):
    assert len(dag_bag.import_errors) == 0, f"DAG Import Errors: {dag_bag.import_errors}"


def test_dag_structure(dag_bag):
    dag_id = 'driver_daily_revenue_etl'

    assert dag_id in dag_bag.dags
    dag = dag_bag.get_dag(dag_id)

    assert dag is not None
    assert len(dag.tasks) == 2, "There must be 2 tasks"

    task_ids = [task.task_id for task in dag.tasks]
    assert 'init_postgres_schema' in task_ids
    assert 'calculate_revenue_and_load' in task_ids


def test_dag_dependencies(dag_bag):
    dag = dag_bag.get_dag('driver_daily_revenue_etl')

    init_task = dag.get_task('init_postgres_schema')
    calc_task = dag.get_task('calculate_revenue_and_load')

    assert calc_task.task_id in init_task.downstream_task_ids