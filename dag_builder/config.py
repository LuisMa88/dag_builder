"""Configuration handling for the dag_builder pipeline."""

import os
from typing import Optional, Dict, Any

import yaml
from pydantic import BaseModel, HttpUrl, ValidationError

from .logger import DagBuilderLogger


logger = DagBuilderLogger.get_logger(__name__)


class PipelineSchema(BaseModel):
    """Pydantic schema for pipeline configuration."""

    # Pipeline Metadata
    pipeline_name: str
    task_id: str = "ingest_api_data"

    # Connection & API Details
    api_url: HttpUrl
    table_name: str
    
    # REST API Specifics (optional)
    api_params: Optional[Dict[str, Any]] = {}
    api_headers: Optional[Dict[str, str]] = {}
    pagination_type: Optional[str] = "offset"
    
    # DuckDB Specifics (optional)
    duckdb_config: Optional[Dict[str, Any]] = {}
    
    # Common settings
    incremental_cursor: str = "updated_at"

class PipelineConfig:  # pylint: disable=too-few-public-methods
    """Loads and validates pipeline configuration from YAML.

    The configuration is validated via Pydantic and secrets are pulled from
    environment variables (e.g. APP_API_TOKEN).
    """

    def __init__(self, path: Optional[str] = None):
        # 1. Determine the path to the config file
        default_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        self.config_path = path or os.getenv('PIPELINE_CONFIG_PATH', default_path)
        logger.debug("Using config path: %s", self.config_path)

        if not os.path.exists(self.config_path):
            logger.error("Config file not found: %s", self.config_path)
            raise FileNotFoundError(f"Config not found at {self.config_path}")

        # 2. Load and validate with Pydantic
        with open(self.config_path, 'r', encoding="utf-8") as f:
            raw_data = yaml.safe_load(f)

        try:
            self.model = PipelineSchema(**raw_data)
        except ValidationError as e:
            # Provides a readable error message of what is missing in the YAML
            logger.error("Invalid configuration in %s: %s", self.config_path, e)
            raise ValueError(f"Invalid Configuration in {self.config_path}:\n{e}") from e

        logger.info("Loaded config for pipeline_name=%s", self.model.pipeline_name)

        # 3. Handle Secrets (keep out of YAML)
        self.api_token = os.getenv('APP_API_TOKEN')
        if not self.api_token:
            logger.error("Missing APP_API_TOKEN environment variable")
            raise EnvironmentError("Missing APP_API_TOKEN")

    def get(self, key, default=None):
        """Helper to access validated attributes."""
        return getattr(self.model, key, default)
