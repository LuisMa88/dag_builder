"""Tests for DuckDB target functionality."""

import os
import tempfile
from unittest.mock import Mock, patch

# Ensure the logger writes to a temporary file during tests
os.environ.setdefault("DAG_BUILDER_LOG_FILE", os.path.join(tempfile.gettempdir(), "dag_builder_test.log"))

from dag_builder.target import DuckDBTarget


class TestDuckDBTarget:
    """Test cases for DuckDBTarget class."""

    def test_init_memory(self):
        """Test DuckDBTarget initialization for in-memory database."""
        target = DuckDBTarget(memory=True)
        
        assert target.memory is True
        assert target.database_path is None
        assert target.read_only is False

    def test_init_file_based(self):
        """Test DuckDBTarget initialization for file-based database."""
        target = DuckDBTarget(database_path="test.duckdb", read_only=True)
        
        assert target.memory is False
        assert target.database_path == "test.duckdb"
        assert target.read_only is True

    def test_get_uri_memory(self):
        """Test URI generation for in-memory database."""
        target = DuckDBTarget(memory=True)
        uri = target.get_uri()
        
        assert uri == "duckdb:///:memory:"

    def test_get_uri_file_based(self):
        """Test URI generation for file-based database."""
        target = DuckDBTarget(database_path="test.duckdb")
        uri = target.get_uri()
        
        expected_path = os.path.abspath("test.duckdb")
        assert uri == f"duckdb:///{expected_path}"

    def test_get_uri_file_based_readonly(self):
        """Test URI generation for read-only file-based database."""
        target = DuckDBTarget(database_path="test.duckdb", read_only=True)
        uri = target.get_uri()
        
        expected_path = os.path.abspath("test.duckdb")
        assert uri == f"duckdb:///{expected_path}?read_only=true"

    def test_get_uri_default_memory(self):
        """Test URI generation defaults to in-memory when no path specified."""
        target = DuckDBTarget()
        uri = target.get_uri()
        
        assert uri == "duckdb:///:memory:"

    @patch('dag_builder.target.BaseHook.get_connection')
    def test_from_connection_success(self, mock_get_connection):
        """Test creating DuckDBTarget from Airflow connection."""
        mock_conn = Mock()
        mock_conn.schema = "custom.duckdb"
        mock_conn.conn_type = "duckdb_file"
        mock_conn.extra = '{"read_only": "false"}'
        mock_get_connection.return_value = mock_conn

        target = DuckDBTarget.from_connection("duckdb_conn")
        
        assert target.database_path == "custom.duckdb"
        assert target.memory is False
        assert target.read_only is False

    @patch('dag_builder.target.BaseHook.get_connection')
    def test_from_connection_memory(self, mock_get_connection):
        """Test creating DuckDBTarget from Airflow connection for memory."""
        mock_conn = Mock()
        mock_conn.conn_type = "duckdb_memory"
        mock_conn.extra = '{"read_only": "false"}'
        mock_get_connection.return_value = mock_conn

        target = DuckDBTarget.from_connection("duckdb_conn")
        
        assert target.memory is True
        assert target.read_only is False

    @patch('dag_builder.target.BaseHook.get_connection')
    def test_from_connection_readonly(self, mock_get_connection):
        """Test creating DuckDBTarget from Airflow connection with read-only."""
        mock_conn = Mock()
        mock_conn.schema = "data.duckdb"
        mock_conn.conn_type = "duckdb_file"
        mock_conn.extra = '{"read_only": "true"}'
        mock_get_connection.return_value = mock_conn

        target = DuckDBTarget.from_connection("duckdb_conn")
        
        assert target.database_path == "data.duckdb"
        assert target.read_only is True

    @patch('dag_builder.target.BaseHook.get_connection')
    def test_from_connection_fallback(self, mock_get_connection):
        """Test fallback to in-memory when connection fails."""
        mock_get_connection.side_effect = Exception("Connection not found")

        target = DuckDBTarget.from_connection("invalid_conn")
        
        assert target.memory is True
        assert target.database_path is None

    @patch('dag_builder.target.BaseHook.get_connection')
    def test_from_connection_with_default_path(self, mock_get_connection):
        """Test creating DuckDBTarget with default path."""
        mock_conn = Mock()
        mock_conn.schema = None  # No schema specified
        mock_conn.conn_type = "duckdb_file"
        mock_conn.extra = '{}'
        mock_get_connection.return_value = mock_conn

        target = DuckDBTarget.from_connection("duckdb_conn", default_path="default.duckdb")
        
        assert target.database_path == "default.duckdb"

    def test_absolute_path_handling(self):
        """Test that relative paths are converted to absolute paths."""
        target = DuckDBTarget(database_path="./data/test.duckdb")
        uri = target.get_uri()
        
        expected_path = os.path.abspath("./data/test.duckdb")
        assert uri == f"duckdb:///{expected_path}"
