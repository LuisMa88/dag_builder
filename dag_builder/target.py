"""Helpers to build connection URIs for Impala targets."""

from airflow.sdk.bases.hook import BaseHook  # pylint: disable=no-name-in-module

from .logger import DagBuilderLogger


logger = DagBuilderLogger.get_logger(__name__)


class ImpalaTarget:  # pylint: disable=too-few-public-methods
    """Generates a SQLAlchemy connection URI for Impala from an Airflow connection."""

    def __init__(self, conn_id):
        self.conn_id = conn_id
        logger.debug("ImpalaTarget initialized with conn_id=%s", conn_id)

    def get_uri(self):
        """Return a SQLAlchemy-compatible connection string for Impala."""
        conn = BaseHook.get_connection(self.conn_id)
        user_pass = f"{conn.login}:{conn.password}@" if conn.login else ""
        port = conn.port or 21050
        db = conn.schema or 'default'
        uri = f"impala://{user_pass}{conn.host}:{port}/{db}"
        logger.info("Generated Impala URI for conn_id=%s", self.conn_id)
        return uri
