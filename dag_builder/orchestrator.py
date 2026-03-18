"""Airflow operator that runs a dlt pipeline from GraphQL to Impala."""

import dlt
from airflow.models import BaseOperator

from .config import PipelineConfig
from .fetcher import GraphQLFetcher
from .target import ImpalaTarget


class DltGraphqlToImpalaOperator(BaseOperator):
    """Airflow operator wrapping a dlt pipeline run."""

    template_fields = ("config_path",)

    def __init__(self, config_path=None, **kwargs):
        super().__init__(**kwargs)
        self.config_path = config_path

    def execute(self, context):
        # 1. Composition setup
        cfg = PipelineConfig(self.config_path)
        fetcher = GraphQLFetcher(
            url=str(cfg.get('api_url')),
            token=cfg.api_token,
            query=cfg.get('graphql_query')
        )
        target = ImpalaTarget(conn_id=cfg.get('airflow_conn_id'))

        # 2. dlt Pipeline Initialization
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
        self.log.info(f"Load complete: {load_info}")
        return str(load_info)
