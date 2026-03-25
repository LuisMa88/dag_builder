"""Tests for dag_builder.config."""
import os
import tempfile
from unittest.mock import patch

# Ensure the logger writes to a temporary file during tests
os.environ.setdefault("DAG_BUILDER_LOG_FILE", os.path.join(tempfile.gettempdir(), "dag_builder_test.log"))

import pytest
import yaml
from dag_builder.config import PipelineConfig, PipelineSchema


class TestPipelineConfig:
    """Test cases for PipelineConfig class."""

    def test_config_validation_error(self, tmp_path):
        """Ensure PipelineConfig validates required fields and raises on invalid input."""
        # Create a config missing the required 'api_url'
        config_file = tmp_path / "invalid_config.yaml"
        config_file.write_text(yaml.dump({"dag_id": "test_dag"}))

        os.environ["APP_API_TOKEN"] = "fake_token"
        with pytest.raises(ValueError, match="Invalid Configuration"):
            PipelineConfig(path=str(config_file))

    def test_config_missing_token(self, tmp_path):
        """Ensure PipelineConfig errors when APP_API_TOKEN environment variable is absent."""
        config_file = tmp_path / "valid.yaml"
        config_file.write_text(yaml.dump({
            "dag_id": "test",
            "airflow_conn_id": "conn",
            "api_url": "http://test.com",
            "table_name": "tab",
            "graphql_query": "query {}"
        }))

        if "APP_API_TOKEN" in os.environ:
            del os.environ["APP_API_TOKEN"]
        with pytest.raises(EnvironmentError, match="Missing APP_API_TOKEN"):
            PipelineConfig(path=str(config_file))

    def test_config_file_not_found(self, tmp_path):
        """Ensure PipelineConfig raises FileNotFoundError when config file is missing."""
        missing_path = tmp_path / "does_not_exist.yaml"
        with pytest.raises(FileNotFoundError) as exc:
            PipelineConfig(path=str(missing_path))
        assert str(missing_path) in str(exc.value)

    def test_config_valid_graphql(self, tmp_path):
        """Test loading valid GraphQL configuration."""
        config_file = tmp_path / "valid_graphql.yaml"
        config_file.write_text(yaml.dump({
            "dag_id": "test_dag",
            "airflow_conn_id": "test_conn",
            "api_url": "http://test.com/graphql",
            "table_name": "test_table",
            "graphql_query": "query { data { id } }"
        }))

        os.environ["APP_API_TOKEN"] = "test_token"
        config = PipelineConfig(path=str(config_file))

        assert config.get("dag_id") == "test_dag"
        assert config.get("airflow_conn_id") == "test_conn"
        assert str(config.get("api_url")) == "http://test.com/graphql"
        assert config.get("table_name") == "test_table"
        assert config.get("graphql_query") == "query { data { id } }"
        assert config.api_token == "test_token"

    def test_config_valid_rest_api(self, tmp_path):
        """Test loading valid REST API configuration."""
        config_file = tmp_path / "valid_rest.yaml"
        config_file.write_text(yaml.dump({
            "dag_id": "rest_dag",
            "airflow_conn_id": "rest_conn",
            "api_url": "http://api.com/data",
            "table_name": "api_table",
            "api_params": {"status": "active", "limit": 100},
            "api_headers": {"User-Agent": "test-agent"},
            "pagination_type": "offset"
        }))

        os.environ["APP_API_TOKEN"] = "rest_token"
        config = PipelineConfig(path=str(config_file))

        assert config.get("dag_id") == "rest_dag"
        assert config.get("pagination_type") == "offset"
        assert config.get("api_params") == {"status": "active", "limit": 100}
        assert config.get("api_headers") == {"User-Agent": "test-agent"}
        assert config.get("graphql_query") is None  # Optional field should be None

    def test_config_with_custom_values(self, tmp_path):
        """Test configuration with custom values for optional fields."""
        config_file = tmp_path / "custom.yaml"
        config_file.write_text(yaml.dump({
            "dag_id": "custom_dag",
            "task_id": "custom_task",
            "schedule": "@hourly",
            "start_date": "2024-01-15",
            "catchup": True,
            "airflow_conn_id": "custom_conn",
            "api_url": "http://custom.com",
            "table_name": "custom_table",
            "graphql_query": "query { custom }",
            "incremental_cursor": "modified_at",
            "api_params": {"category": "products"},
            "pagination_type": "cursor"
        }))

        os.environ["APP_API_TOKEN"] = "custom_token"
        config = PipelineConfig(path=str(config_file))

        assert config.get("task_id") == "custom_task"
        assert config.get("schedule") == "@hourly"
        assert config.get("start_date") == "2024-01-15"
        assert config.get("catchup") is True
        assert config.get("incremental_cursor") == "modified_at"
        assert config.get("pagination_type") == "cursor"

    def test_config_default_values(self, tmp_path):
        """Test configuration with default values for optional fields."""
        config_file = tmp_path / "defaults.yaml"
        config_file.write_text(yaml.dump({
            "dag_id": "defaults_dag",
            "airflow_conn_id": "defaults_conn",
            "api_url": "http://defaults.com",
            "table_name": "defaults_table",
            "graphql_query": "query { defaults }"
        }))

        os.environ["APP_API_TOKEN"] = "defaults_token"
        config = PipelineConfig(path=str(config_file))

        # Check default values
        assert config.get("task_id") == "ingest_api_data"
        assert config.get("schedule") == "@daily"
        assert config.get("start_date") == "2024-01-01"
        assert config.get("catchup") is False
        assert config.get("incremental_cursor") == "updated_at"
        assert config.get("api_params") == {}
        assert config.get("api_headers") == {}
        assert config.get("pagination_type") == "offset"

    @patch.dict(os.environ, {"PIPELINE_CONFIG_PATH": "/path/to/env/config.yaml"})
    def test_config_path_from_environment(self, tmp_path):
        """Test that config path is read from PIPELINE_CONFIG_PATH when no path is provided."""
        # This test verifies the path resolution logic, but doesn't test file loading
        # since we're mocking the environment variable
        with patch("dag_builder.config.os.path.exists", return_value=True), \
             patch("dag_builder.config.open", create=True) as mock_open, \
             patch("dag_builder.config.yaml.safe_load", return_value={
                 "dag_id": "env_dag",
                 "airflow_conn_id": "env_conn",
                 "api_url": "http://env.com",
                 "table_name": "env_table",
                 "graphql_query": "query { env }"
             }):
            
            os.environ["APP_API_TOKEN"] = "env_token"
            config = PipelineConfig()  # No path provided
            
            # Verify the environment path was used
            mock_open.assert_called_once_with("/path/to/env/config.yaml", 'r', encoding="utf-8")

    def test_config_invalid_url(self, tmp_path):
        """Test validation of invalid URL format."""
        config_file = tmp_path / "invalid_url.yaml"
        config_file.write_text(yaml.dump({
            "dag_id": "invalid_url_dag",
            "airflow_conn_id": "conn",
            "api_url": "not-a-valid-url",
            "table_name": "table",
            "graphql_query": "query {}"
        }))

        os.environ["APP_API_TOKEN"] = "token"
        with pytest.raises(ValueError, match="Invalid Configuration"):
            PipelineConfig(path=str(config_file))

    def test_config_get_method(self, tmp_path):
        """Test the get method for accessing configuration values."""
        config_file = tmp_path / "get_test.yaml"
        config_file.write_text(yaml.dump({
            "dag_id": "get_test_dag",
            "airflow_conn_id": "get_conn",
            "api_url": "http://get.com",
            "table_name": "get_table",
            "graphql_query": "query { get }"
        }))

        os.environ["APP_API_TOKEN"] = "get_token"
        config = PipelineConfig(path=str(config_file))

        # Test getting various attributes
        assert config.get("dag_id") == "get_test_dag"
        assert config.get("airflow_conn_id") == "get_conn"
        assert str(config.get("api_url")).startswith("http://get.com")
        assert config.get("table_name") == "get_table"
        assert config.get("graphql_query") == "query { get }"


class TestPipelineSchema:
    """Test cases for PipelineSchema validation."""

    def test_schema_validation_required_fields(self):
        """Test that required fields are validated."""
        # Missing required fields should raise validation error
        with pytest.raises(Exception):  # Pydantic ValidationError
            PipelineSchema()

    def test_schema_with_minimal_required_fields(self):
        """Test schema with only required fields."""
        schema = PipelineSchema(
            dag_id="test",
            api_url="http://test.com",
            airflow_conn_id="conn",
            table_name="table",
            graphql_query="query {}"
        )
        
        assert schema.dag_id == "test"
        assert schema.task_id == "ingest_api_data"  # Default value
        assert schema.schedule == "@daily"  # Default value
        assert schema.api_params == {}  # Default value
        assert schema.pagination_type == "offset"  # Default value

    def test_schema_with_rest_api_only(self):
        """Test schema with REST API configuration (no GraphQL query)."""
        schema = PipelineSchema(
            dag_id="rest_test",
            api_url="http://rest.com",
            airflow_conn_id="rest_conn",
            table_name="rest_table"
            # graphql_query is optional
        )
        
        assert schema.graphql_query is None
        assert schema.pagination_type == "offset"  # Default value
