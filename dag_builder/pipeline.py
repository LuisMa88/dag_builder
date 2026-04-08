"""Data pipeline runners for DLT pipelines without Airflow."""

import dlt
import os
from .config import PipelineConfig
from .fetcher import RestApiFetcher
from .logger import DagBuilderLogger
from .target import DuckDBTarget


logger = DagBuilderLogger.get_logger(__name__)


class DataPipeline:
    """Simple data pipeline runner for DLT pipelines."""

    def __init__(self, config_path: str):
        """
        Initialize the pipeline with configuration.
        
        Args:
            config_path: Path to the YAML configuration file
        """
        self.config_path = config_path
        self.cfg = PipelineConfig(config_path)
        
    def run_rest_api_to_duckdb(self, working_dir: str = None):
        """
        Run REST API to DuckDB pipeline.
        
        Args:
            working_dir: Directory to change to before running (for DLT config)
        """
        logger.info("Starting REST API to DuckDB pipeline; config_path=%s", self.config_path)

        # Store original working directory
        original_cwd = os.getcwd()
        print(f"Original working directory: {original_cwd}")
        
        try:
            # Change working directory if specified
            if working_dir:
                os.chdir(working_dir)
                print(f"Changed working directory to: {working_dir}")

            # 1. Setup fetcher
            fetcher = RestApiFetcher(
                url=str(self.cfg.get('api_url')),
                token=self.cfg.api_token,
                params=self.cfg.get('api_params', {}),
                headers=self.cfg.get('api_headers', {}),
                pagination_type=self.cfg.get('pagination_type', 'offset')
            )

            # 2. Setup target
            target_config = self.cfg.get('duckdb_config', {})
            if 'destination_name' in target_config:
                target = DuckDBTarget(
                    # destination_name=target_config.get('destination_name', self.cfg.get('pipeline_name') + '.duckdb'),
                    read_only=target_config.get('read_only', False)
                )
            else:
                # Default to in-memory for DuckDB
                target = DuckDBTarget(memory=True)

            # 3. DLT Pipeline Initialization
            logger.info("Initializing dlt pipeline for dag_id=%s", self.cfg.get('dag_id'))
            
            # Ensure the DuckDB data directory exists if specified
            if hasattr(target, 'destination_name') and target.destination_name:
                duckdb_data_dir = os.path.dirname(target.destination_name)
                os.makedirs(duckdb_data_dir, exist_ok=True)
            
            pipeline = dlt.pipeline(
                pipeline_name=self.cfg.get('pipeline_name'),
                # destination="duckdb",
                destination=dlt.destinations.duckdb(
                    destination_name=target_config.get('destination_name', self.cfg.get('pipeline_name') + '.duckdb')
                ),


                # database=target_config.get('destination_name', self.cfg.get('pipeline_name') + '.duckdb'),
                dataset_name=target_config.get('dataset_name', self.cfg.get('pipeline_name'))
            )

            # 4. Resource Definition with Incremental Loading
            resource = dlt.resource(
                fetcher.fetch_records(
                    dlt.sources.incremental(self.cfg.get('incremental_cursor'))
                ),
                name=self.cfg.get('table_name'),
                write_disposition="merge",
                primary_key="id"
            )

            # 5. Run the pipeline
            logger.info("Running data pipeline...")
            load_info = pipeline.run(resource)
            
            logger.info("Load complete: %s", load_info)
            
            # Return information about where data was stored
            result = {
                'pipeline_name': self.cfg.get('pipeline_name'),
                'load_info': load_info,
                'destination_name': getattr(target, 'destination_name', None),
                'table_name': self.cfg.get('table_name')
            }
            
            return result

        finally:
            # Restore original working directory
            os.chdir(original_cwd)


