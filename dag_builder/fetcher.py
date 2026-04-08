"""REST API data fetching utilities for the dag_builder pipeline."""

import dlt
from dlt.sources.helpers import requests

from .logger import DagBuilderLogger


logger = DagBuilderLogger.get_logger(__name__)


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
        total_fetched = 0
        max_records = self.params.get("_limit", 1000)  # Default max 1000 records
        
        logger.info("Starting fetch from %s (since=%s, max_records=%s)", self.url, since, max_records)

        while has_next and total_fetched < max_records:
            # Prepare request parameters
            request_params = self.params.copy()
            
            # Add incremental filtering
            if since:
                request_params["since"] = since
            
            # Add pagination parameters based on type
            if self.pagination_type == "offset":
                request_params["offset"] = offset
                # Calculate remaining records needed
                remaining = max_records - total_fetched
                request_params["limit"] = min(100, remaining)  # Don't fetch more than needed
            elif self.pagination_type == "page":
                request_params["page"] = page
                remaining = max_records - total_fetched
                request_params["per_page"] = min(100, remaining)  # Don't fetch more than needed
            elif self.pagination_type == "cursor":
                if cursor:
                    request_params["cursor"] = cursor
                remaining = max_records - total_fetched
                request_params["limit"] = min(100, remaining)  # Don't fetch more than needed
            
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

            logger.info("Fetched %s records (page=%s, offset=%s, total=%s/%s)", len(records), page, offset, total_fetched + len(records), max_records)
            if records:
                yield records
                total_fetched += len(records)

            # Update pagination for next iteration
            if self.pagination_type == "offset":
                offset += len(records)
            elif self.pagination_type == "page":
                page += 1
            # cursor is updated in the loop above for cursor-based pagination
            
            # Safety check to prevent infinite loops
            if not records or total_fetched >= max_records:
                break
