"""Tests for dag_builder.target."""
import os
import tempfile

# Ensure the logger writes to a temporary file during tests
os.environ.setdefault("DAG_BUILDER_LOG_FILE", os.path.join(tempfile.gettempdir(), "dag_builder_test.log"))

from dag_builder.target import ImpalaTarget

def test_impala_uri_generation(mocker):
    """Validate that ImpalaTarget builds a proper Impala URI from a connection."""
    # Mock Airflow BaseHook
    mock_conn = mocker.patch("airflow.sdk.bases.hook.BaseHook.get_connection")
    mock_conn.return_value.host = "localhost"
    mock_conn.return_value.login = "user"
    mock_conn.return_value.password = "pass"
    mock_conn.return_value.port = 21050
    mock_conn.return_value.schema = "prod_db"

    target = ImpalaTarget(conn_id="my_conn")
    uri = target.get_uri()

    assert uri == "impala://user:pass@localhost:21050/prod_db"
