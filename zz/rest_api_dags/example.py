# """Example Airflow DAG using the dag_builder REST API operator."""

# from datetime import datetime

# from airflow import DAG
# from dag_builder.orchestrator import DltRestApiToImpalaOperator

# with DAG(
#     dag_id="rest_api_data_ingest",
#     start_date=datetime(2024, 1, 1),
#     schedule="@daily",
#     catchup=False,
#     tags=["rest_api", "example"],
# ) as dag:

#     DltRestApiToImpalaOperator(
#         task_id="ingest_rest_api_data",
#         config_path="config.yaml",
#     )
