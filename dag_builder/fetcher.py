"""GraphQL and REST API data fetching utilities for the dag_builder pipeline."""

import dlt
from dlt.sources.helpers import requests

from .logger import DagBuilderLogger


logger = DagBuilderLogger.get_logger(__name__)


class GraphQLFetcher:  # pylint: disable=too-few-public-methods
    """Fetches paginated records from a GraphQL API and yields record batches."""

    def __init__(self, url, token, query):
        self.url = url
        self.headers = {"Authorization": f"Bearer {token}"}
        self.query = query
        logger.debug("Initialized GraphQLFetcher for URL %s", self.url)

    def fetch_records(self, last_value=dlt.sources.incremental("updated_at")):
        """Yield pages of records from the configured GraphQL endpoint.

        Args:
            last_value: incremental state used to request only newly updated data.
        """
        cursor = None
        has_next = True
        since = last_value.start_value
        logger.info("Starting fetch from %s (since=%s)", self.url, since)

        while has_next:
            payload = {
                'query': self.query,
                'variables': {'cursor': cursor, 'since': since}
            }
            logger.debug("Posting GraphQL payload: %s", payload)
            response = requests.post(self.url, json=payload, headers=self.headers)
            response.raise_for_status()

            # Navigate GraphQL response (customize based on your specific API schema)
            data = response.json().get("data", {})
            # Dynamically find the first list of nodes if possible, or use a config path
            resource_data = next(iter(data.values()), {})

            nodes = resource_data.get("nodes", [])
            logger.info("Fetched %s records (cursor=%s)", len(nodes), cursor)
            if nodes:
                yield nodes

            page_info = resource_data.get("pageInfo", {})
            has_next = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")


class RestApiFetcher:  # pylint: disable=too-few-public-methods
    """Fetches paginated records from a REST API and yields record batches."""

    def __init__(self, url, token=None, params=None, headers=None, pagination_type="offset"):
        """Initialize REST API fetcher.
        
        Args:
            url: Base URL for the REST API endpoint
            token: Optional bearer token for authentication
            params: Optional query parameters for requests
            headers: Optional additional headers
            pagination_type: Type of pagination ('offset', 'cursor', or 'page')
        """
        self.url = url
        self.params = params or {}
        self.pagination_type = pagination_type
        
        # Set up headers with optional authentication
        self.headers = headers or {}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        
        logger.debug("Initialized RestApiFetcher for URL %s (pagination: %s)", self.url, pagination_type)

    def fetch_records(self, last_value=dlt.sources.incremental("updated_at")):
        """Yield pages of records from the configured REST endpoint.
        
        Args:
            last_value: incremental state used to request only newly updated data.
        """
        page = 1
        offset = 0
        cursor = None
        has_next = True
        since = last_value.start_value
        
        logger.info("Starting fetch from %s (since=%s)", self.url, since)

        while has_next:
            # Prepare request parameters
            request_params = self.params.copy()
            
            # Add incremental filtering
            if since:
                request_params["since"] = since
            
            # Add pagination parameters based on type
            if self.pagination_type == "offset":
                request_params["offset"] = offset
                request_params["limit"] = 100  # Default page size
            elif self.pagination_type == "page":
                request_params["page"] = page
                request_params["per_page"] = 100  # Default page size
            elif self.pagination_type == "cursor":
                if cursor:
                    request_params["cursor"] = cursor
                request_params["limit"] = 100  # Default page size
            
            logger.debug("Requesting %s with params: %s", self.url, request_params)
            response = requests.get(self.url, params=request_params, headers=self.headers)
            response.raise_for_status()

            data = response.json()
            
            # Handle different response formats
            if isinstance(data, list):
                # Direct array response
                records = data
                has_next = len(records) > 0
            elif isinstance(data, dict):
                # Object response - look for common data keys
                records = (
                    data.get("data") or 
                    data.get("results") or 
                    data.get("items") or 
                    data.get("records") or 
                    []
                )
                
                # Check for pagination metadata
                pagination = data.get("pagination") or data.get("meta") or {}
                if self.pagination_type == "offset":
                    total = pagination.get("total") or pagination.get("count")
                    if total is not None:
                        has_next = offset + len(records) < total
                    else:
                        has_next = len(records) > 0
                elif self.pagination_type == "page":
                    has_next = pagination.get("has_next", len(records) > 0)
                elif self.pagination_type == "cursor":
                    cursor = pagination.get("next_cursor") or pagination.get("cursor")
                    has_next = bool(cursor) or len(records) > 0
                else:
                    has_next = len(records) > 0
            else:
                records = []
                has_next = False

            logger.info("Fetched %s records (page=%s, offset=%s)", len(records), page, offset)
            if records:
                yield records

            # Update pagination for next iteration
            if self.pagination_type == "offset":
                offset += len(records)
            elif self.pagination_type == "page":
                page += 1
            # cursor is updated in the loop above for cursor-based pagination
            
            # Safety check to prevent infinite loops
            if not records:
                break
