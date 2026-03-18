"""GraphQL data fetching utilities for the dag_builder pipeline."""

import dlt
from dlt.sources.helpers import requests


class GraphQLFetcher:  # pylint: disable=too-few-public-methods
    """Fetches paginated records from a GraphQL API and yields record batches."""

    def __init__(self, url, token, query):
        self.url = url
        self.headers = {"Authorization": f"Bearer {token}"}
        self.query = query

    def fetch_records(self, last_value=dlt.sources.incremental("updated_at")):
        """Yield pages of records from the configured GraphQL endpoint.

        Args:
            last_value: incremental state used to request only newly updated data.
        """
        cursor = None
        has_next = True
        since = last_value.start_value

        while has_next:
            payload = {
                'query': self.query,
                'variables': {'cursor': cursor, 'since': since}
            }
            response = requests.post(self.url, json=payload, headers=self.headers)
            response.raise_for_status()

            # Navigate GraphQL response (customize based on your specific API schema)
            data = response.json().get("data", {})
            # Dynamically find the first list of nodes if possible, or use a config path
            resource_data = next(iter(data.values()), {})

            nodes = resource_data.get("nodes", [])
            if nodes:
                yield nodes

            page_info = resource_data.get("pageInfo", {})
            has_next = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
