"""Tests for dag_builder.fetcher."""
import os
import tempfile
import pytest
from unittest.mock import Mock, patch

# Ensure the logger writes to a temporary file during tests
os.environ.setdefault("DAG_BUILDER_LOG_FILE", os.path.join(tempfile.gettempdir(), "dag_builder_test.log"))

from dag_builder.fetcher import GraphQLFetcher


class TestGraphQLFetcher:
    """Test cases for GraphQLFetcher class."""

    def test_init(self):
        """Test GraphQLFetcher initialization."""
        fetcher = GraphQLFetcher(
            url="http://api.com",
            token="test_token",
            query="query { data }"
        )
        
        assert fetcher.url == "http://api.com"
        assert fetcher.headers["Authorization"] == "Bearer test_token"
        assert fetcher.query == "query { data }"

    @patch('dag_builder.fetcher.requests.post')
    def test_fetch_records_single_page(self, mock_post):
        """Test fetching records from a single page GraphQL response."""
        mock_post.return_value.json.return_value = {
            "data": {
                "analytics": {
                    "nodes": [{"id": 1, "val": "A"}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None}
                }
            }
        }
        mock_post.return_value.raise_for_status = Mock()

        fetcher = GraphQLFetcher("http://api.com", "token", "query {}")
        records = list(fetcher.fetch_records())

        assert len(records) == 1
        assert records[0][0]["id"] == 1
        assert mock_post.called

    @patch('dag_builder.fetcher.requests.post')
    def test_fetch_records_multiple_pages(self, mock_post):
        """Test fetching records from multiple pages."""
        # First page response
        first_response = Mock()
        first_response.json.return_value = {
            "data": {
                "analytics": {
                    "nodes": [{"id": 1, "val": "A"}],
                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor123"}
                }
            }
        }
        first_response.raise_for_status = Mock()

        # Second page response
        second_response = Mock()
        second_response.json.return_value = {
            "data": {
                "analytics": {
                    "nodes": [{"id": 2, "val": "B"}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None}
                }
            }
        }
        second_response.raise_for_status = Mock()

        mock_post.side_effect = [first_response, second_response]

        fetcher = GraphQLFetcher("http://api.com", "token", "query {}")
        records = list(fetcher.fetch_records())

        assert len(records) == 2
        assert records[0][0]["id"] == 1
        assert records[1][0]["id"] == 2
        assert mock_post.call_count == 2

    @patch('dag_builder.fetcher.requests.post')
    def test_fetch_records_with_incremental_filtering(self, mock_post):
        """Test fetching records with incremental filtering."""
        mock_post.return_value.json.return_value = {
            "data": {
                "analytics": {
                    "nodes": [{"id": 1, "updated_at": "2024-01-02"}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None}
                }
            }
        }
        mock_post.return_value.raise_for_status = Mock()

        fetcher = GraphQLFetcher("http://api.com", "token", "query {}")
        
        # Mock the incremental source with a start value
        mock_incremental = Mock()
        mock_incremental.start_value = "2024-01-01"
        
        records = list(fetcher.fetch_records(mock_incremental))

        # Verify the request included the 'since' parameter
        call_args = mock_post.call_args
        payload = call_args[1]['json']
        assert payload['variables']['since'] == "2024-01-01"

    @patch('dag_builder.fetcher.requests.post')
    def test_fetch_records_http_error(self, mock_post):
        """Test handling of HTTP errors."""
        mock_post.return_value.raise_for_status.side_effect = Exception("HTTP 500 Error")

        fetcher = GraphQLFetcher("http://api.com", "token", "query {}")
        
        with pytest.raises(Exception, match="HTTP 500 Error"):
            list(fetcher.fetch_records())

    @patch('dag_builder.fetcher.requests.post')
    def test_fetch_records_empty_nodes(self, mock_post):
        """Test handling of empty nodes response."""
        mock_post.return_value.json.return_value = {
            "data": {
                "analytics": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None}
                }
            }
        }
        mock_post.return_value.raise_for_status = Mock()

        fetcher = GraphQLFetcher("http://api.com", "token", "query {}")
        records = list(fetcher.fetch_records())

        assert len(records) == 0

    @patch('dag_builder.fetcher.requests.post')
    def test_fetch_records_missing_data(self, mock_post):
        """Test handling of missing data in response."""
        mock_post.return_value.json.return_value = {
            "data": {
                "analytics": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None}
                }
            }
        }
        mock_post.return_value.raise_for_status = Mock()

        fetcher = GraphQLFetcher("http://api.com", "token", "query {}")
        records = list(fetcher.fetch_records())

        assert len(records) == 0

    @patch('dag_builder.fetcher.requests.post')
    def test_fetch_records_dynamic_resource_discovery(self, mock_post):
        """Test dynamic discovery of resource data."""
        mock_post.return_value.json.return_value = {
            "data": {
                "someOtherResource": {
                    "nodes": [{"id": 1, "val": "A"}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None}
                }
            }
        }
        mock_post.return_value.raise_for_status = Mock()

        fetcher = GraphQLFetcher("http://api.com", "token", "query {}")
        records = list(fetcher.fetch_records())

        assert len(records) == 1
        assert records[0][0]["id"] == 1

    @patch('dag_builder.fetcher.requests.post')
    def test_fetch_records_with_cursor_pagination(self, mock_post):
        """Test cursor-based pagination flow."""
        # First call with no cursor
        first_call = Mock()
        first_call.json.return_value = {
            "data": {
                "analytics": {
                    "nodes": [{"id": 1}],
                    "pageInfo": {"hasNextPage": True, "endCursor": "abc123"}
                }
            }
        }
        first_call.raise_for_status = Mock()

        # Second call with cursor
        second_call = Mock()
        second_call.json.return_value = {
            "data": {
                "analytics": {
                    "nodes": [{"id": 2}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None}
                }
            }
        }
        second_call.raise_for_status = Mock()

        mock_post.side_effect = [first_call, second_call]

        fetcher = GraphQLFetcher("http://api.com", "token", "query {}")
        records = list(fetcher.fetch_records())

        # Verify first call had no cursor
        first_payload = mock_post.call_args_list[0][1]['json']
        assert first_payload['variables']['cursor'] is None

        # Verify second call had cursor
        second_payload = mock_post.call_args_list[1][1]['json']
        assert second_payload['variables']['cursor'] == "abc123"

        assert len(records) == 2
        assert records[0][0]["id"] == 1
        assert records[1][0]["id"] == 2
