"""dag_builder - A data pipeline package for building DLT pipelines."""

from .config import PipelineConfig
from .fetcher import RestApiFetcher
from .pipeline import DataPipeline
from .target import DuckDBTarget

__version__ = "0.1.0"

__all__ = [
    "PipelineConfig",
    "RestApiFetcher",
    "DataPipeline",
    "DuckDBTarget"
]