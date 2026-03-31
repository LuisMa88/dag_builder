"""Airflow operators that run dlt pipelines from GraphQL/REST APIs to Impala."""

import dlt
from airflow.models import BaseOperator

from .config import PipelineConfig
from .fetcher import GraphQLFetcher, RestApiFetcher
from .logger import DagBuilderLogger
from .target import ImpalaTarget, DuckDBTarget


logger = DagBuilderLogger.get_logger(__name__)


class DltGraphqlToImpalaOperator(BaseOperator):
    """Airflow operator wrapping a dlt pipeline run."""

    template_fields = ("config_path",)

    def __init__(self, config_path=None, **kwargs):
        super().__init__(**kwargs)
        self.config_path = config_path

    def execute(self, context):
        logger.info("Starting DAG run; config_path=%s", self.config_path)

        # 1. Composition setup
        cfg = PipelineConfig(self.config_path)
        fetcher = GraphQLFetcher(
            url=str(cfg.get('api_url')),
            token=cfg.api_token,
            query=cfg.get('graphql_query')
        )
        target = ImpalaTarget(conn_id=cfg.get('airflow_conn_id'))

        # 2. dlt Pipeline Initialization
        logger.info("Initializing dlt pipeline for dag_id=%s", cfg.get('dag_id'))
        pipeline = dlt.pipeline(
            pipeline_name=cfg.get('dag_id'),
            destination="sqlalchemy",
            credentials=target.get_uri(),
            dataset_name="staging"
        )

        # 3. Resource Definition with Incremental Loading
        resource = dlt.resource(
            fetcher.fetch_records(
                dlt.sources.incremental(cfg.get('incremental_cursor'))
            ),
            name=cfg.get('table_name'),
            write_disposition="merge",
            primary_key="id"
        )

        # 4. Run
        load_info = pipeline.run(resource)
        logger.info("Load complete: %s", load_info)
        self.log.info(f"Load complete: {load_info}")
        
        # Log data ingestion details
        try:
            # Connect to database to verify data was loaded
            import duckdb
            db_path = target_config.get('database_path', '/opt/airflow/data/posts_data.duckdb')
            logger.info(f"🔍 Connecting to database: {db_path}")
            conn = duckdb.connect(db_path)
            
            # Check if table exists and get record count
            tables = conn.execute("SHOW TABLES").fetchall()
            if tables:
                table_name = cfg.get('table_name')
                count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchall()
                logger.info(f"✅ Successfully loaded {count_result[0][0]} records into table '{table_name}'")
                
                # Log sample data
                sample_data = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                logger.info(f"📋 Sample data: {sample_data}")
                
                # Log table schema
                schema = conn.execute(f"DESCRIBE {table_name}").fetchall()
                logger.info(f"🏗️ Table schema: {schema}")
            else:
                logger.warning("⚠️ No tables found in database after load")
            
            conn.close()
        except Exception as e:
            logger.error(f"❌ Error verifying loaded data: {e}")
        
        return str(load_info)


class DltGraphqlToDuckDBOperator(BaseOperator):
    """Airflow operator wrapping a dlt pipeline run for GraphQL to DuckDB."""

    template_fields = ("config_path",)

    def __init__(self, config_path=None, **kwargs):
        super().__init__(**kwargs)
        self.config_path = config_path

    def execute(self, context):
        logger.info("Starting DAG run; config_path=%s", self.config_path)

        # 1. Composition setup
        cfg = PipelineConfig(self.config_path)
        fetcher = GraphQLFetcher(
            url=str(cfg.get('api_url')),
            token=cfg.api_token,
            query=cfg.get('graphql_query')
        )
        
        # 2. DuckDB Target Setup
        target_config = cfg.get('duckdb_config', {})
        if target_config.get('memory', False):
            target = DuckDBTarget(memory=True)
        elif target_config.get('database_path'):
            target = DuckDBTarget(
                database_path=target_config['database_path'],
                read_only=target_config.get('read_only', False)
            )
        else:
            # Default to in-memory for DuckDB
            target = DuckDBTarget(memory=True)

        # 3. dlt Pipeline Initialization
        logger.info("Initializing dlt pipeline for dag_id=%s", cfg.get('dag_id'))
        # For DuckDB, set the database path as environment variable
        db_path = target_config.get('database_path', '/opt/airflow/data/posts_data.duckdb')
        # Use absolute path and ensure directory exists
        import os
        abs_db_path = os.path.abspath(db_path)
        os.makedirs(os.path.dirname(abs_db_path), exist_ok=True)
        
        # Set DuckDB database path as environment variable for DLT
        os.environ['DUCKDB_DATABASE'] = abs_db_path
        
        pipeline = dlt.pipeline(
            pipeline_name=cfg.get('dag_id'),
            destination="duckdb",
            dataset_name="staging"
        )

        # 4. Resource Definition with Incremental Loading
        resource = dlt.resource(
            fetcher.fetch_records(
                dlt.sources.incremental(cfg.get('incremental_cursor'))
            ),
            name=cfg.get('table_name'),
            write_disposition="merge",
            primary_key="id"
        )

        # 5. Run
        load_info = pipeline.run(resource)
        logger.info("Load complete: %s", load_info)
        self.log.info(f"Load complete: {load_info}")
        
        # Log data ingestion details
        try:
            # Connect to database to verify data was loaded
            import duckdb
            db_path = target_config.get('database_path', '/opt/airflow/data/posts_data.duckdb')
            logger.info(f"🔍 Connecting to database: {db_path}")
            conn = duckdb.connect(db_path)
            
            # Check if table exists and get record count
            tables = conn.execute("SHOW TABLES").fetchall()
            if tables:
                table_name = cfg.get('table_name')
                count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchall()
                logger.info(f"✅ Successfully loaded {count_result[0][0]} records into table '{table_name}'")
                
                # Log sample data
                sample_data = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                logger.info(f"📋 Sample data: {sample_data}")
                
                # Log table schema
                schema = conn.execute(f"DESCRIBE {table_name}").fetchall()
                logger.info(f"🏗️ Table schema: {schema}")
            else:
                logger.warning("⚠️ No tables found in database after load")
            
            conn.close()
        except Exception as e:
            logger.error(f"❌ Error verifying loaded data: {e}")
        
        return str(load_info)


class DltRestApiToDuckDBOperator(BaseOperator):
    """Airflow operator wrapping a dlt pipeline run for REST APIs to DuckDB."""

    template_fields = ("config_path",)

    def __init__(self, config_path=None, **kwargs):
        super().__init__(**kwargs)
        self.config_path = config_path

    def execute(self, context):
        logger.info("Starting DAG run; config_path=%s", self.config_path)

        # 1. Composition setup
        cfg = PipelineConfig(self.config_path)
        fetcher = RestApiFetcher(
            url=str(cfg.get('api_url')),
            token=cfg.api_token,
            params=cfg.get('api_params', {}),
            headers=cfg.get('api_headers', {}),
            pagination_type=cfg.get('pagination_type', 'offset')
        )
        
        # 2. DuckDB Target Setup
        target_config = cfg.get('duckdb_config', {})
        if target_config.get('memory', False):
            target = DuckDBTarget(memory=True)
        elif target_config.get('database_path'):
            target = DuckDBTarget(
                database_path=target_config['database_path'],
                read_only=target_config.get('read_only', False)
            )
        else:
            # Default to in-memory for DuckDB
            target = DuckDBTarget(memory=True)

        # 3. dlt Pipeline Initialization
        logger.info("Initializing dlt pipeline for dag_id=%s", cfg.get('dag_id'))
        # For DuckDB, set the database path as environment variable
        db_path = target_config.get('database_path', '/opt/airflow/data/posts_data.duckdb')
        # Use absolute path and ensure directory exists
        import os
        abs_db_path = os.path.abspath(db_path)
        os.makedirs(os.path.dirname(abs_db_path), exist_ok=True)
        
        # Set DuckDB database path as environment variable for DLT
        os.environ['DUCKDB_DATABASE'] = abs_db_path
        
        pipeline = dlt.pipeline(
            pipeline_name=cfg.get('dag_id'),
            destination="duckdb",
            dataset_name="staging"
        )

        # 4. Resource Definition with Incremental Loading
        resource = dlt.resource(
            fetcher.fetch_records(
                dlt.sources.incremental(cfg.get('incremental_cursor'))
            ),
            name=cfg.get('table_name'),
            write_disposition="merge",
            primary_key="id"
        )

        # 4. Run
        load_info = pipeline.run(resource)
        logger.info("Load complete: %s", load_info)
        self.log.info(f"Load complete: {load_info}")
        
        # Log data ingestion details
        try:
            # Connect to database to verify data was loaded
            import duckdb
            db_path = target_config.get('database_path', '/opt/airflow/data/posts_data.duckdb')
            logger.info(f"🔍 Connecting to database: {db_path}")
            conn = duckdb.connect(db_path)
            
            # Check if table exists and get record count
            tables = conn.execute("SHOW TABLES").fetchall()
            if tables:
                table_name = cfg.get('table_name')
                count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchall()
                logger.info(f"✅ Successfully loaded {count_result[0][0]} records into table '{table_name}'")
                
                # Log sample data
                sample_data = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                logger.info(f"📋 Sample data: {sample_data}")
                
                # Log table schema
                schema = conn.execute(f"DESCRIBE {table_name}").fetchall()
                logger.info(f"🏗️ Table schema: {schema}")
            else:
                logger.warning("⚠️ No tables found in database after load")
            
            conn.close()
        except Exception as e:
            logger.error(f"❌ Error verifying loaded data: {e}")
        
        return str(load_info)
