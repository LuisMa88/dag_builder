"""Tests for the dag_builder logger functionality."""

import os

import pytest

from dag_builder.logger import DagBuilderLogger


def test_default_file_logger_writes_to_file(tmp_path, monkeypatch):
    """The default logger should write to a file when DAG_BUILDER_LOG_DEST is unset."""
    log_path = tmp_path / "dag_builder.log"
    monkeypatch.delenv("DAG_BUILDER_LOG_DEST", raising=False)
    monkeypatch.setenv("DAG_BUILDER_LOG_FILE", str(log_path))

    DagBuilderLogger.reset()
    logger = DagBuilderLogger.get_logger("test")
    logger.info("hello world")

    # Ensure file was created and contains the message
    assert log_path.exists(), "Expected the log file to be created"
    content = log_path.read_text(encoding="utf-8")
    assert "hello world" in content


def test_opensearch_logger_uses_requests_post(monkeypatch):
    """When configured for OpenSearch, the logger should post documents to OpenSearch."""
    monkeypatch.setenv("DAG_BUILDER_LOG_DEST", "opensearch")
    monkeypatch.setenv("DAG_BUILDER_OPENSEARCH_URL", "https://opensearch.example")
    monkeypatch.setenv("DAG_BUILDER_OPENSEARCH_INDEX", "test_index")
    monkeypatch.setenv("DAG_BUILDER_OPENSEARCH_USER", "user")
    monkeypatch.setenv("DAG_BUILDER_OPENSEARCH_PASSWORD", "pass")

    DagBuilderLogger.reset()

    called = {}

    def fake_post(url, json=None, auth=None, timeout=None):
        called["url"] = url
        called["json"] = json
        called["auth"] = auth
        called["timeout"] = timeout
        class Dummy:
            status_code = 201
        return Dummy()

    monkeypatch.setattr("dag_builder.logger.requests.post", fake_post)

    logger = DagBuilderLogger.get_logger("test")
    logger.warning("opensearch test")

    assert called["url"] == "https://opensearch.example/test_index/_doc"
    assert called["auth"] == ("user", "pass")
    assert "opensearch test" in called["json"]["message"]
