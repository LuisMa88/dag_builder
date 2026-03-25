"""Tests for RestApiFetcher functionality."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from dag_builder.fetcher import RestApiFetcher


class TestRestApiFetcher:
    """Test cases for RestApiFetcher class."""

    def test_init_with_token(self):
        """Test RestApiFetcher initialization with token."""
        fetcher = RestApiFetcher(
            url="https://api.example.com/data",
            token="test_token",
            pagination_type="page"
        )
        
        assert fetcher.url == "https://api.example.com/data"
        assert fetcher.pagination_type == "page"
        assert fetcher.headers["Authorization"] == "Bearer test_token"
        assert fetcher.params == {}

    def test_init_without_token(self):
        """Test RestApiFetcher initialization without token."""
        fetcher = RestApiFetcher(
            url="https://api.example.com/data",
            headers={"Custom-Header": "value"}
        )
        
        assert fetcher.url == "https://api.example.com/data"
        assert fetcher.headers["Custom-Header"] == "value"
        assert "Authorization" not in fetcher.headers

    def test_init_with_params_and_headers(self):
        """Test RestApiFetcher initialization with params and headers."""
        fetcher = RestApiFetcher(
            url="https://api.example.com/data",
            token="token123",
            params={"status": "active", "limit": 50},
            headers={"User-Agent": "test-agent"}
        )
        
        assert fetcher.params["status"] == "active"
        assert fetcher.params["limit"] == 50
        assert fetcher.headers["User-Agent"] == "test-agent"
        assert fetcher.headers["Authorization"] == "Bearer token123"

    @patch('dag_builder.fetcher.requests.get')
    def test_fetch_records_offset_pagination(self, mock_get):
        """Test fetching records with offset pagination."""
        # Mock response with direct array
        mock_response = Mock()
        mock_response.json.return_value = [
            {"id": 1, "name": "Item 1"},
            {"id": 2, "name": "Item 2"}
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetcher = RestApiFetcher(
            url="https://api.example.com/data",
            pagination_type="offset"
        )

        records = list(fetcher.fetch_records())
        
        # Should make one request and return the records
        assert len(records) == 1
        assert len(records[0]) == 2
        assert records[0][0]["id"] == 1

    @patch('dag_builder.fetcher.requests.get')
    def test_fetch_records_page_pagination(self, mock_get):
        """Test fetching records with page pagination."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "name": "Item 1"},
                {"id": 2, "name": "Item 2"}
            ],
            "pagination": {
                "has_next": False
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetcher = RestApiFetcher(
            url="https://api.example.com/data",
            pagination_type="page"
        )

        records = list(fetcher.fetch_records())
        
        assert len(records) == 1
        assert len(records[0]) == 2
        assert records[0][0]["id"] == 1

    @patch('dag_builder.fetcher.requests.get')
    def test_fetch_records_cursor_pagination(self, mock_get):
        """Test fetching records with cursor pagination."""
        # First response
        first_response = Mock()
        first_response.json.return_value = {
            "data": [
                {"id": 1, "name": "Item 1"}
            ],
            "pagination": {
                "next_cursor": "abc123",
                "has_more": True
            }
        }
        first_response.raise_for_status.return_value = None

        # Second response (final page)
        second_response = Mock()
        second_response.json.return_value = {
            "data": [
                {"id": 2, "name": "Item 2"}
            ],
            "pagination": {
                "next_cursor": None,
                "has_more": False
            }
        }
        second_response.raise_for_status.return_value = None

        mock_get.side_effect = [first_response, second_response]

        fetcher = RestApiFetcher(
            url="https://api.example.com/data",
            pagination_type="cursor"
        )

        records = list(fetcher.fetch_records())
        
        # Should make two requests and return all records
        assert len(records) == 2
        assert len(records[0]) == 1  # First page
        assert len(records[1]) == 1  # Second page
        assert records[0][0]["id"] == 1
        assert records[1][0]["id"] == 2
        assert mock_get.call_count == 2

    @patch('dag_builder.fetcher.requests.get')
    def test_fetch_records_with_params(self, mock_get):
        """Test fetching records with custom parameters."""
        mock_response = Mock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetcher = RestApiFetcher(
            url="https://api.example.com/data",
            params={"status": "active", "limit": 50}
        )

        list(fetcher.fetch_records())
        
        # Verify the request was made with custom params + pagination params
        mock_get.assert_called_once()
        called_params = mock_get.call_args[1]['params']
        assert called_params['status'] == "active"
        assert called_params['limit'] == 50
        assert 'offset' in called_params

    @patch('dag_builder.fetcher.requests.get')
    def test_fetch_records_multiple_response_formats(self, mock_get):
        """Test fetching records with different response formats."""
        test_cases = [
            # Direct array
            ([{"id": 1}], [{"id": 1}]),
            
            # Wrapped with "data"
            ({"data": [{"id": 1}]}, [{"id": 1}]),
            
            # Wrapped with "results"
            ({"results": [{"id": 1}]}, [{"id": 1}]),
            
            # Wrapped with "items"
            ({"items": [{"id": 1}]}, [{"id": 1}]),
            
            # Wrapped with "records"
            ({"records": [{"id": 1}]}, [{"id": 1}]),
        ]

        for response_data, expected_records in test_cases:
            mock_response = Mock()
            mock_response.json.return_value = response_data
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            fetcher = RestApiFetcher(url="https://api.example.com/data")
            records = list(fetcher.fetch_records())
            
            assert len(records) == 1
            assert records[0] == expected_records

    @patch('dag_builder.fetcher.requests.get')
    def test_fetch_records_with_incremental_filtering(self, mock_get):
        """Test fetching records with incremental filtering."""
        mock_response = Mock()
        mock_response.json.return_value = {"data": [{"id": 1, "updated_at": "2024-01-02"}]}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetcher = RestApiFetcher(url="https://api.example.com/data")
        
        # Mock the incremental source with a start value
        mock_incremental = Mock()
        mock_incremental.start_value = "2024-01-01"
        
        list(fetcher.fetch_records(mock_incremental))
        
        # Verify the request included the 'since' parameter
        called_params = mock_get.call_args[1]['params']
        assert called_params['since'] == "2024-01-01"

    @patch('dag_builder.fetcher.requests.get')
    def test_fetch_records_http_error(self, mock_get):
        """Test handling of HTTP errors."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("HTTP 500 Error")
        mock_get.return_value = mock_response

        fetcher = RestApiFetcher(url="https://api.example.com/data")
        
        with pytest.raises(Exception, match="HTTP 500 Error"):
            list(fetcher.fetch_records())

    @patch('dag_builder.fetcher.requests.get')
    def test_fetch_records_empty_response(self, mock_get):
        """Test handling of empty responses."""
        mock_response = Mock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetcher = RestApiFetcher(url="https://api.example.com/data")
        records = list(fetcher.fetch_records())
        
        assert len(records) == 0

    @patch('dag_builder.fetcher.requests.get')
    def test_fetch_records_invalid_response_format(self, mock_get):
        """Test handling of invalid response formats."""
        mock_response = Mock()
        mock_response.json.return_value = {"invalid": "format"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetcher = RestApiFetcher(url="https://api.example.com/data")
        records = list(fetcher.fetch_records())
        
        assert len(records) == 0

    @patch('dag_builder.fetcher.requests.get')
    def test_fetch_records_offset_pagination_with_total(self, mock_get):
        """Test offset pagination when total count is provided."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [{"id": 1}, {"id": 2}],
            "pagination": {
                "total": 4,
                "count": 2
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetcher = RestApiFetcher(
            url="https://api.example.com/data",
            pagination_type="offset"
        )

        records = list(fetcher.fetch_records())
        
        # Should continue pagination since offset (0) + count (2) < total (4)
        assert mock_get.call_count >= 2

    @patch('dag_builder.fetcher.requests.get')
    def test_fetch_records_infinite_loop_prevention(self, mock_get):
        """Test that empty responses prevent infinite loops."""
        mock_response = Mock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetcher = RestApiFetcher(url="https://api.example.com/data")
        records = list(fetcher.fetch_records())
        
        # Should make only one request and stop
        assert mock_get.call_count == 1
        assert len(records) == 0
