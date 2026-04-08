"""Data pipeline runners for DLT pipelines without Airflow."""

import dlt
import os
from .config import PipelineConfig
from .fetcher import GraphQLFetcher, RestApiFetcher
from .logger import DagBuilderLogger
from .target import ImpalaTarget, DuckDBTarget


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
        
        try:
            # Change working directory if specified
            if working_dir:
                os.chdir(working_dir)

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
            if 'database_path' in target_config:
                target = DuckDBTarget(
                    database_path=target_config['database_path'],
                    read_only=target_config.get('read_only', False)
                )
            else:
                # Default to in-memory for DuckDB
                target = DuckDBTarget(memory=True)

            # 3. DLT Pipeline Initialization
            logger.info("Initializing dlt pipeline for dag_id=%s", self.cfg.get('dag_id'))
            
            # Ensure the DuckDB data directory exists if specified
            if hasattr(target, 'database_path') and target.database_path:
                duckdb_data_dir = os.path.dirname(target.database_path)
                os.makedirs(duckdb_data_dir, exist_ok=True)
            
            pipeline = dlt.pipeline(
                pipeline_name=self.cfg.get('dag_id'),
                destination="duckdb",
                dataset_name="staging"
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
                'pipeline_name': self.cfg.get('dag_id'),
                'load_info': load_info,
                'database_path': getattr(target, 'database_path', None),
                'table_name': self.cfg.get('table_name')
            }
            
            return result

        finally:
            # Restore original working directory
            os.chdir(original_cwd)

    def run_graphql_to_impala(self):
        """Run GraphQL to Impala pipeline."""
        logger.info("Starting GraphQL to Impala pipeline; config_path=%s", self.config_path)

        # 1. Composition setup
        fetcher = GraphQLFetcher(
            url=str(self.cfg.get('api_url')),
            token=self.cfg.api_token,
            query=self.cfg.get('graphql_query')
        )
        target = ImpalaTarget(conn_id=self.cfg.get('airflow_conn_id'))

        # 2. dlt Pipeline Initialization
        logger.info("Initializing dlt pipeline for dag_id=%s", self.cfg.get('dag_id'))
        pipeline = dlt.pipeline(
            pipeline_name=self.cfg.get('dag_id'),
            destination="sqlalchemy",
            credentials=target.get_uri(),
            dataset_name="staging"
        )

        # 3. Resource Definition with Incremental Loading
        resource = dlt.resource(
            fetcher.fetch_records(
                dlt.sources.incremental(self.cfg.get('incremental_cursor'))
            ),
            name=self.cfg.get('table_name'),
            write_disposition="merge",
            primary_key="id"
        )

        # 4. Run the pipeline
        load_info = pipeline.run(resource)
        logger.info("Load complete: %s", load_info)
        
        return {
            'pipeline_name': self.cfg.get('dag_id'),
            'load_info': load_info,
            'table_name': self.cfg.get('table_name')
        }


def run_pipeline(config_path: str, pipeline_type: str = "rest_api_to_duckdb", working_dir: str = None):
    """
    Convenience function to run a pipeline.
    
    Args:
        config_path: Path to the YAML configuration file
        pipeline_type: Type of pipeline to run ("rest_api_to_duckdb" or "graphql_to_impala")
        working_dir: Directory to change to before running (for DLT config)
    
    Returns:
        Dictionary with pipeline results
    """
    pipeline = DataPipeline(config_path)
    
    if pipeline_type == "rest_api_to_duckdb":
        return pipeline.run_rest_api_to_duckdb(working_dir)
    elif pipeline_type == "graphql_to_impala":
        return pipeline.run_graphql_to_impala()
    else:
        raise ValueError(f"Unknown pipeline type: {pipeline_type}")
