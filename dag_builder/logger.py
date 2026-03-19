"""Singleton logging utilities for dag_builder.

This module provides a single shared logger instance that can be configured to
write to a file (default) or send logs to an OpenSearch cluster.

Configuration is driven via environment variables:

- DAG_BUILDER_LOG_DEST: "file" (default) or "opensearch".
- DAG_BUILDER_LOG_FILE: path to the log file (default: dag_builder.log).
- DAG_BUILDER_OPENSEARCH_URL: base URL of OpenSearch (e.g. https://host:9200).
- DAG_BUILDER_OPENSEARCH_INDEX: index name to write logs to (default: dag_builder_logs).
- DAG_BUILDER_OPENSEARCH_USER / DAG_BUILDER_OPENSEARCH_PASSWORD: optional basic auth.

"""

from __future__ import annotations

import logging
import os
from typing import Optional

import requests


DEFAULT_LOG_DEST = "file"
DEFAULT_LOG_FILE = "dag_builder.log"
DEFAULT_OPENSEARCH_INDEX = "dag_builder_logs"


class OpenSearchHandler(logging.Handler):
    """Logging handler that writes records to OpenSearch."""

    def __init__(self, url: str, index: str, auth: Optional[tuple[str, str]] = None, timeout: float = 5.0):
        super().__init__()
        self.url = url.rstrip("/")
        self.index = index
        self.auth = auth
        self.timeout = timeout

    def emit(self, record: logging.LogRecord) -> None:
        try:
            # Build a minimal document for OpenSearch
            timestamp = None
            if self.formatter:
                timestamp = self.formatter.formatTime(record)
            else:
                timestamp = logging.Formatter().formatTime(record)

            doc = {
                "@timestamp": timestamp,
                "logger": record.name,
                "level": record.levelname,
                "message": record.getMessage(),
                "module": record.module,
                "funcName": record.funcName,
                "lineno": record.lineno,
            }
            endpoint = f"{self.url}/{self.index}/_doc"
            requests.post(endpoint, json=doc, auth=self.auth, timeout=self.timeout)
        except Exception:
            self.handleError(record)


class DagBuilderLogger:
    """Singleton logger for dagger builder.

    The singleton is lazily configured the first time get_logger() is called.
    """

    _instance: Optional["DagBuilderLogger"] = None

    def __init__(self):
        self._logger = logging.getLogger("dag_builder")
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False
        self._configure_handlers()

    @classmethod
    def get_instance(cls) -> "DagBuilderLogger":
        if cls._instance is None:
            cls._instance = DagBuilderLogger()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton (primarily for tests)."""
        if cls._instance is not None:
            # Remove handlers to avoid writing logs to old destinations
            for h in list(cls._instance._logger.handlers):
                cls._instance._logger.removeHandler(h)
            cls._instance = None

    @classmethod
    def get_logger(cls, name: Optional[str] = None) -> logging.Logger:
        """Return a logger instance.

        If a name is provided, a child logger will be returned (e.g.
        "dag_builder.<name>").
        """
        inst = cls.get_instance()
        if name:
            return inst._logger.getChild(name)
        return inst._logger

    def _configure_handlers(self) -> None:
        # Avoid adding handlers multiple times when tests run in the same process
        if self._logger.handlers:
            return

        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

        dest = os.getenv("DAG_BUILDER_LOG_DEST", DEFAULT_LOG_DEST).lower().strip()
        if dest == "opensearch":
            url = os.getenv("DAG_BUILDER_OPENSEARCH_URL")
            if url:
                index = os.getenv("DAG_BUILDER_OPENSEARCH_INDEX", DEFAULT_OPENSEARCH_INDEX)
                user = os.getenv("DAG_BUILDER_OPENSEARCH_USER")
                password = os.getenv("DAG_BUILDER_OPENSEARCH_PASSWORD")
                auth = (user, password) if user and password else None
                handler = OpenSearchHandler(url=url, index=index, auth=auth)
                handler.setFormatter(formatter)
                self._logger.addHandler(handler)
                return

            # fall back to file when OpenSearch isn't configured
            file_handler = self._create_file_handler(formatter)
            self._logger.addHandler(file_handler)
            self._logger.warning("DAG_BUILDER_LOG_DEST=opensearch but DAG_BUILDER_OPENSEARCH_URL is not set; falling back to file logging")
            return

        file_handler = self._create_file_handler(formatter)
        self._logger.addHandler(file_handler)

    def _create_file_handler(self, formatter: logging.Formatter) -> logging.Handler:
        log_file = os.getenv("DAG_BUILDER_LOG_FILE", DEFAULT_LOG_FILE)
        handler = logging.FileHandler(log_file)
        handler.setFormatter(formatter)
        return handler
