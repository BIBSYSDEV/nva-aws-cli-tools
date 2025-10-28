import boto3
import requests


class SearchApiService:
    def __init__(self, profile):
        self.session = boto3.Session(profile_name=profile)
        self.ssm = self.session.client("ssm")
        self.api_domain = self._get_system_parameter("/NVA/ApiDomain")

    def _get_system_parameter(self, name):
        response = self.ssm.get_parameter(Name=name)
        return response["Parameter"]["Value"]

    def get_uri(self, type):
        return f"https://{self.api_domain}/search/{type}"

    def resource_search(self, query_parameters, page_size=100):
        """
        Search resources with automatic pagination.

        Args:
            query_parameters: Dictionary of query parameters (without 'from' and 'results')
            page_size: Number of results per page (default: 100)

        Yields:
            Individual hits from the search results
        """
        url = f"{self.get_uri("resources")}"
        headers = {
            "Accept": "application/json; version=2024-12-01",
        }
        offset = 0

        while True:
            params = {
                **query_parameters,
                "from": offset,
                "results": page_size,
            }

            response = requests.get(url, headers=headers, params=params)

            if response.status_code != 200:
                print(
                    f"Failed to search. {response.status_code}: {response.json()}",
                    response.status_code,
                )
                break

            response_data = response.json()
            hits = response_data.get("hits", [])

            if not hits:
                break

            for hit in hits:
                yield hit

            # Check if we've retrieved all results
            total_hits = response_data.get("totalHits", 0)
            offset += len(hits)

            if offset >= total_hits:
                break

