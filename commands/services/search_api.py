import boto3
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


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

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(
            (requests.exceptions.RequestException, requests.exceptions.HTTPError)
        ),
        reraise=True,
    )
    def _make_search_request(self, url, headers, params, debug=False):
        """
        Make a search request with retry logic.

        Args:
            url: The API endpoint URL
            headers: Request headers
            params: Query parameters
            debug: If True, print debug information

        Returns:
            Response object

        Raises:
            requests.exceptions.RequestException: On network errors
            requests.exceptions.HTTPError: On HTTP errors (500, 502, 503, 504)
        """
        if debug:
            import sys

            print(f"[DEBUG] URL: {url}", file=sys.stderr)
            print(f"[DEBUG] Params: {params}", file=sys.stderr)
            print(f"[DEBUG] Headers: {headers}", file=sys.stderr)

        response = requests.get(url, headers=headers, params=params, timeout=30)

        if debug:
            import sys

            print(f"[DEBUG] Status: {response.status_code}", file=sys.stderr)
            print(f"[DEBUG] Full URL: {response.url}", file=sys.stderr)

        if response.status_code >= 500:
            if debug:
                import sys

                print(
                    f"[DEBUG] Server error {response.status_code}, will retry...",
                    file=sys.stderr,
                )
            response.raise_for_status()

        return response

    def resource_search(
        self, query_parameters, page_size=100, debug=False, api_version="2024-12-01"
    ):
        """
        Search resources with automatic pagination.

        Args:
            query_parameters: Dictionary of query parameters (without 'from' and 'results')
            page_size: Number of results per page (default: 100)
            debug: If True, print debug information including URLs
            api_version: API version to use in Accept header (default: 2024-12-01)

        Yields:
            Individual hits from the search results
        """
        url = f"{self.get_uri("resources")}"
        headers = {
            "Accept": f"application/json; version={api_version}",
        }
        offset = 0

        while True:
            params = {
                **query_parameters,
                "from": offset,
                "results": page_size,
            }

            try:
                response = self._make_search_request(url, headers, params, debug)
            except requests.exceptions.HTTPError as e:
                import sys

                print(
                    f"Failed to search after retries. Status: {e.response.status_code}",
                    file=sys.stderr,
                )
                if e.response.status_code >= 400:
                    try:
                        error_detail = e.response.json()
                        print(f"Error detail: {error_detail}", file=sys.stderr)
                    except:
                        print(f"Error detail: {e.response.text}", file=sys.stderr)
                break
            except requests.exceptions.RequestException as e:
                import sys

                print(f"Network error after retries: {e}", file=sys.stderr)
                break

            if response.status_code != 200:
                import sys

                print(
                    f"Failed to search. {response.status_code}: {response.json()}",
                    file=sys.stderr,
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
