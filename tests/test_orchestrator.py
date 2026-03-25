"""Tests for dag_builder.orchestrator."""
import os
import tempfile
from unittest.mock import Mock, patch

# Ensure the logger writes to a temporary file during tests
os.environ.setdefault("DAG_BUILDER_LOG_FILE", os.path.join(tempfile.gettempdir(), "dag_builder_test.log"))

from dag_builder.orchestrator import DltGraphqlToImpalaOperator, DltRestApiToImpalaOperator, DltGraphqlToDuckDBOperator, DltRestApiToDuckDBOperator


class TestDltGraphqlToImpalaOperator:
    """Test cases for DltGraphqlToImpalaOperator."""

    def test_operator_init(self):
        """Test operator initialization."""
        op = DltGraphqlToImpalaOperator(
            task_id="test_task",
            config_path="/path/to/config.yaml"
        )
        
        assert op.task_id == "test_task"
        assert op.config_path == "/path/to/config.yaml"

    @patch('dag_builder.target.BaseHook.get_connection')
    @patch('dlt.pipeline')
    def test_operator_execute(self, mock_pipeline, mock_get_connection, tmp_path):
        """Ensure the operator runs a dlt pipeline against the configured API."""
        # Setup valid config file
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("""
dag_id: "test_dag"
airflow_conn_id: "impala_conn"
api_url: "http://api.com"
table_name: "test_table"
graphql_query: "query {}"
""")
        os.environ["APP_API_TOKEN"] = "token"

        # Mock connection
        mock_conn = Mock()
        mock_conn.get_uri.return_value = "impala://localhost:21050"
        mock_get_connection.return_value = mock_conn

        # Instantiate and run operator
        op = DltGraphqlToImpalaOperator(task_id="test_task", config_path=str(cfg_file))
        result = op.execute(context={})

        # Verify dlt was called
        assert mock_pipeline.called
        assert mock_pipeline.return_value.run.called
        assert result is not None

    @patch('dag_builder.target.BaseHook.get_connection')
    @patch('dlt.pipeline')
    def test_operator_execute_with_custom_config(self, mock_pipeline, mock_get_connection, tmp_path):
        """Test operator with custom configuration values."""
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("""
dag_id: "custom_dag"
task_id: "custom_task"
schedule: "@hourly"
start_date: "2024-01-15"
catchup: true
airflow_conn_id: "custom_impala_conn"
api_url: "http://custom-api.com"
table_name: "custom_table"
graphql_query: "query { customData }"
incremental_cursor: "modified_at"
""")
        os.environ["APP_API_TOKEN"] = "custom_token"

        # Mock connection
        mock_conn = Mock()
        mock_conn.get_uri.return_value = "impala://custom-host:21050"
        mock_get_connection.return_value = mock_conn

        op = DltGraphqlToImpalaOperator(task_id="test_task", config_path=str(cfg_file))
        result = op.execute(context={})

        assert mock_pipeline.called
        assert result is not None


class TestDltRestApiToImpalaOperator:
    """Test cases for DltRestApiToImpalaOperator."""

    def test_operator_init(self):
        """Test REST API operator initialization."""
        op = DltRestApiToImpalaOperator(
            task_id="test_rest_task",
            config_path="/path/to/rest_config.yaml"
        )
        
        assert op.task_id == "test_rest_task"
        assert op.config_path == "/path/to/rest_config.yaml"

    @patch('dag_builder.target.BaseHook.get_connection')
    @patch('dlt.pipeline')
    def test_rest_api_operator_execute(self, mock_pipeline, mock_get_connection, tmp_path):
        """Ensure the REST API operator runs a dlt pipeline."""
        cfg_file = tmp_path / "rest_config.yaml"
        cfg_file.write_text("""
dag_id: "rest_dag"
airflow_conn_id: "impala_conn"
api_url: "http://rest-api.com/data"
table_name: "rest_table"
api_params:
  status: "active"
  limit: 100
api_headers:
  User-Agent: "test-agent"
pagination_type: "offset"
incremental_cursor: "updated_at"
""")
        os.environ["APP_API_TOKEN"] = "rest_token"

        # Mock connection
        mock_conn = Mock()
        mock_conn.get_uri.return_value = "impala://localhost:21050"
        mock_get_connection.return_value = mock_conn

        op = DltRestApiToImpalaOperator(task_id="test_rest_task", config_path=str(cfg_file))
        result = op.execute(context={})

        # Verify dlt was called
        assert mock_pipeline.called
        assert mock_pipeline.return_value.run.called
        assert result is not None

    @patch('dag_builder.target.BaseHook.get_connection')
    @patch('dlt.pipeline')
    def test_rest_api_operator_with_cursor_pagination(self, mock_pipeline, mock_get_connection, tmp_path):
        """Test REST API operator with cursor pagination."""
        cfg_file = tmp_path / "cursor_config.yaml"
        cfg_file.write_text("""
dag_id: "cursor_dag"
airflow_conn_id: "impala_conn"
api_url: "http://api.com/cursor-data"
table_name: "cursor_table"
pagination_type: "cursor"
api_params:
  category: "products"
""")
        os.environ["APP_API_TOKEN"] = "cursor_token"

        # Mock connection
        mock_conn = Mock()
        mock_conn.get_uri.return_value = "impala://localhost:21050"
        mock_get_connection.return_value = mock_conn

        op = DltRestApiToImpalaOperator(task_id="cursor_task", config_path=str(cfg_file))
        result = op.execute(context={})

        assert mock_pipeline.called
        assert result is not None

    @patch('dag_builder.target.BaseHook.get_connection')
    @patch('dlt.pipeline')
    def test_rest_api_operator_minimal_config(self, mock_pipeline, mock_get_connection, tmp_path):
        """Test REST API operator with minimal configuration."""
        cfg_file = tmp_path / "minimal_config.yaml"
        cfg_file.write_text("""
dag_id: "minimal_dag"
airflow_conn_id: "impala_conn"
api_url: "http://api.com/minimal"
table_name: "minimal_table"
""")
        os.environ["APP_API_TOKEN"] = "minimal_token"

        # Mock connection
        mock_conn = Mock()
        mock_conn.get_uri.return_value = "impala://localhost:21050"
        mock_get_connection.return_value = mock_conn

        op = DltRestApiToImpalaOperator(task_id="minimal_task", config_path=str(cfg_file))
        result = op.execute(context={})

        assert mock_pipeline.called
        assert result is not None


class TestDltGraphqlToDuckDBOperator:
    """Test cases for DltGraphqlToDuckDBOperator."""

    def test_operator_init(self):
        """Test DuckDB GraphQL operator initialization."""
        op = DltGraphqlToDuckDBOperator(
            task_id="test_duckdb_task",
            config_path="/path/to/config.yaml"
        )
        
        assert op.task_id == "test_duckdb_task"
        assert op.config_path == "/path/to/config.yaml"

    @patch('dlt.pipeline')
    def test_duckdb_operator_memory_config(self, mock_pipeline, tmp_path):
        """Test DuckDB operator with in-memory configuration."""
        cfg_file = tmp_path / "duckdb_memory_config.yaml"
        cfg_file.write_text("""
dag_id: "duckdb_memory_dag"
airflow_conn_id: "duckdb_conn"
api_url: "http://api.com/graphql"
table_name: "memory_table"
graphql_query: "query { data }"
duckdb_config:
  memory: true
""")
        os.environ["APP_API_TOKEN"] = "duckdb_token"

        op = DltGraphqlToDuckDBOperator(task_id="duckdb_memory_task", config_path=str(cfg_file))
        result = op.execute(context={})

        # Verify dlt was called with duckdb destination
        mock_pipeline.assert_called_once()
        call_args = mock_pipeline.call_args
        assert call_args[1]['destination'] == "duckdb"
        assert result is not None

    @patch('dlt.pipeline')
    def test_duckdb_operator_file_config(self, mock_pipeline, tmp_path):
        """Test DuckDB operator with file-based configuration."""
        cfg_file = tmp_path / "duckdb_file_config.yaml"
        cfg_file.write_text("""
dag_id: "duckdb_file_dag"
airflow_conn_id: "duckdb_conn"
api_url: "http://api.com/graphql"
table_name: "file_table"
graphql_query: "query { data }"
duckdb_config:
  database_path: "test_data.duckdb"
  read_only: false
""")
        os.environ["APP_API_TOKEN"] = "duckdb_token"

        op = DltGraphqlToDuckDBOperator(task_id="duckdb_file_task", config_path=str(cfg_file))
        result = op.execute(context={})

        # Verify dlt was called with duckdb destination
        mock_pipeline.assert_called_once()
        call_args = mock_pipeline.call_args
        assert call_args[1]['destination'] == "duckdb"
        assert result is not None


class TestDltRestApiToDuckDBOperator:
    """Test cases for DltRestApiToDuckDBOperator."""

    def test_operator_init(self):
        """Test REST API DuckDB operator initialization."""
        op = DltRestApiToDuckDBOperator(
            task_id="test_rest_duckdb_task",
            config_path="/path/to/rest_config.yaml"
        )
        
        assert op.task_id == "test_rest_duckdb_task"
        assert op.config_path == "/path/to/rest_config.yaml"

    @patch('dlt.pipeline')
    def test_rest_api_duckdb_operator_execute(self, mock_pipeline, tmp_path):
        """Test REST API DuckDB operator execution."""
        cfg_file = tmp_path / "rest_duckdb_config.yaml"
        cfg_file.write_text("""
dag_id: "rest_duckdb_dag"
airflow_conn_id: "duckdb_conn"
api_url: "http://rest-api.com/data"
table_name: "rest_duckdb_table"
api_params:
  status: "active"
  limit: 100
api_headers:
  User-Agent: "test-agent"
pagination_type: "offset"
duckdb_config:
  memory: true
""")
        os.environ["APP_API_TOKEN"] = "rest_duckdb_token"

        op = DltRestApiToDuckDBOperator(task_id="rest_duckdb_task", config_path=str(cfg_file))
        result = op.execute(context={})

        # Verify dlt was called with duckdb destination
        mock_pipeline.assert_called_once()
        call_args = mock_pipeline.call_args
        assert call_args[1]['destination'] == "duckdb"
        assert result is not None
