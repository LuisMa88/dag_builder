"""Tests for dag_builder.orchestrator."""
import os
import tempfile

# Ensure the logger writes to a temporary file during tests
os.environ.setdefault("DAG_BUILDER_LOG_FILE", os.path.join(tempfile.gettempdir(), "dag_builder_test.log"))

from dag_builder.orchestrator import DltGraphqlToImpalaOperator


def test_operator_execute(mocker, tmp_path):
    """Ensure the operator runs a dlt pipeline against the configured API."""
    # 1. Setup valid config file
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("""
dag_id: "test_dag"
airflow_conn_id: "impala_conn"
api_url: "http://api.com"
table_name: "test_table"
graphql_query: "query {}"
""")
    os.environ["APP_API_TOKEN"] = "token"

    # 2. Mock Internal Components
    mocker.patch("dag_builder.target.BaseHook.get_connection")
    mock_pipeline = mocker.patch("dlt.pipeline")

    # 3. Instantiate and run operator
    op = DltGraphqlToImpalaOperator(task_id="test_task", config_path=str(cfg_file))
    op.execute(context={})

    # 4. Verify dlt was called
    assert mock_pipeline.called
    assert mock_pipeline.return_value.run.called
