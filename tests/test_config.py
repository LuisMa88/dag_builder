"""Tests for dag_builder.config."""
import os

import pytest
import yaml
from dag_builder.config import PipelineConfig

def test_config_validation_error(tmp_path):
    """Ensure PipelineConfig validates required fields and raises on invalid input.

    The config file here omits the required `api_url` field. PipelineConfig should
    raise ValueError with an "Invalid Configuration" message when required fields
    are missing.
    """
    # Create a config missing the required 'api_url'
    config_file = tmp_path / "invalid_config.yaml"
    config_file.write_text(yaml.dump({"dag_id": "test_dag"}))

    os.environ["APP_API_TOKEN"] = "fake_token"
    with pytest.raises(ValueError, match="Invalid Configuration"):
        PipelineConfig(path=str(config_file))


def test_config_missing_token(tmp_path):
    """Ensure PipelineConfig errors when APP_API_TOKEN environment variable is absent.

    PipelineConfig requires APP_API_TOKEN to be set in the environment. When it
    is not present, an EnvironmentError should be raised with a helpful message.
    """
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


def test_config_file_not_found(tmp_path):
    """Ensure PipelineConfig raises FileNotFoundError when config file is missing."""
    missing_path = tmp_path / "does_not_exist.yaml"
    with pytest.raises(FileNotFoundError) as exc:
        PipelineConfig(path=str(missing_path))
    assert str(missing_path) in str(exc.value)
