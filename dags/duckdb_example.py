"""Example Airflow DAG using the dag_builder DuckDB operators."""

from datetime import datetime

from airflow import DAG
from dag_builder.orchestrator import DltGraphqlToDuckDBOperator, DltRestApiToDuckDBOperator

# with DAG(
#     dag_id="duckdb_graphql_ingest",
#     start_date=datetime(2024, 1, 1),
#     schedule="@daily",
#     tags=["duckdb", "graphql", "example"],
# ) as dag:

#     # GraphQL to DuckDB example
#     graphql_to_duckdb = DltGraphqlToDuckDBOperator(
#         task_id="ingest_graphql_to_duckdb",
#         config_path="config.yaml",
#     )

with DAG(
    dag_id="duckdb_rest_api_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["duckdb", "rest_api", "example"],
) as rest_api_dag:

    # REST API to DuckDB example
    rest_api_to_duckdb = DltRestApiToDuckDBOperator(
        task_id="ingest_rest_api_to_duckdb",
        config_path="/opt/airflow/dags/rest_api_config.yaml",
    )
