"""Example Airflow DAG using the dag_builder operator."""

from datetime import datetime

from airflow import DAG

from dag_builder.orchestrator import DltGraphqlToImpalaOperator


with DAG(
    dag_id="marketing_data_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
) as dag:

    DltGraphqlToImpalaOperator(
        task_id="run_ingestion",
        # Use the included example config file
        config_path="/path/to/dag_builder/example/config.yaml",
    )
