"""Tests for dag_builder.fetcher."""
from dag_builder.fetcher import GraphQLFetcher

def test_fetch_records_pagination(mocker):
    """Ensure GraphQLFetcher paginates through API responses correctly.

    This test mocks the underlying HTTP request to return a single page of
    records and verifies that the fetch_records() generator yields the expected
    list of records and that the request was performed.
    """
    # Mock the API response
    mock_post = mocker.patch("dlt.sources.helpers.requests.post")
    mock_post.return_value.json.return_value = {
        "data": {
            "analytics": {
                "nodes": [{"id": 1, "val": "A"}],
                "pageInfo": {"hasNextPage": False, "endCursor": None}
            }
        }
    }
    mock_post.return_value.raise_for_status = mocker.Mock()

    fetcher = GraphQLFetcher("http://api.com", "token", "query {}")
    records = list(fetcher.fetch_records())

    assert len(records) == 1
    assert records[0][0]["id"] == 1
    assert mock_post.called
