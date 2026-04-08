"""dag_builder - A data pipeline package for building DLT pipelines."""

from .config import PipelineConfig
from .fetcher import GraphQLFetcher, RestApiFetcher
from .pipeline import DataPipeline, run_pipeline
from .target import ImpalaTarget, DuckDBTarget

__version__ = "0.1.0"

__all__ = [
    "PipelineConfig",
    "GraphQLFetcher", 
    "RestApiFetcher",
    "DataPipeline",
    "run_pipeline",
    "ImpalaTarget",
    "DuckDBTarget"
]