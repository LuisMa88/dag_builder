"""Helpers to build connection URIs for DuckDB targets."""

import os
from .logger import DagBuilderLogger


logger = DagBuilderLogger.get_logger(__name__)


class DuckDBTarget:  # pylint: disable=too-few-public-methods
    """Generates a connection URI for DuckDB with flexible configuration options."""

    def __init__(self, conn_id=None, destination_name=None, memory=False, read_only=False):
        """Initialize DuckDB target.
        
        Args:
            conn_id: Optional Airflow connection ID (for future use)
            destination_name: Path to DuckDB database file
            memory: Whether to use in-memory database
            read_only: Whether to open database in read-only mode
        """
        self.conn_id = conn_id
        self.destination_name = destination_name
        self.memory = memory
        self.read_only = read_only
        logger.debug("DuckDBTarget initialized: memory=%s, read_only=%s, path=%s", 
                    memory, read_only, destination_name)

    def get_uri(self):
        """Return a SQLAlchemy-compatible connection string for DuckDB."""
        if self.memory:
            uri = "duckdb:///:memory:"
            logger.info("Generated DuckDB in-memory URI")
        elif self.destination_name:
            # Handle absolute and relative paths
            db_path = os.path.abspath(self.destination_name)
            
            # Add read-only flag if specified
            if self.read_only:
                uri = f"duckdb://{db_path}?read_only=true"
            else:
                uri = f"duckdb://{db_path}"
            
            logger.info("Generated DuckDB file URI: %s (read_only=%s)", db_path, self.read_only)
        else:
            # Default to in-memory if no path specified
            uri = "duckdb:///:memory:"
            logger.info("Generated DuckDB default in-memory URI")
        
        return uri

    @classmethod
    def from_connection(cls, conn_id, default_path=None):
        """Create DuckDBTarget from Airflow connection.
        
        Args:
            conn_id: Airflow connection ID
            default_path: Default database path if not specified in connection
            
        Returns:
            DuckDBTarget instance
        """
        try:
            conn = BaseHook.get_connection(conn_id)
            
            # Extract connection parameters
            destination_name = conn.schema or default_path or "duckdb_data.duckdb"
            memory = conn.conn_type == "duckdb_memory"
            read_only = conn.extra and conn.extra.get("read_only", False).lower() == "true"
            
            return cls(
                conn_id=conn_id,
                destination_name=destination_name+".duckdb",
                memory=memory,
                read_only=read_only
            )
        except Exception as e:
            logger.warning("Failed to get DuckDB connection %s: %s", conn_id, e)
            # Fallback to default in-memory database
            return cls(memory=True)
