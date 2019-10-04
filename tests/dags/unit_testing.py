from airflow import models


def assert_has_valid_dag(module):
    """Assert that a module contains a valid DAG."""

    no_dag_found = True

    for dag in vars(module).values():
        if isinstance(dag, models.DAG):
            no_dag_found = False
            dag.test_cycle()  # Throws if a task cycle is found.

    if no_dag_found:
        raise AssertionError("module does not contain a valid DAG")

