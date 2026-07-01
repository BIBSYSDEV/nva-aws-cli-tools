import boto3
import logging
import requests
from requests.exceptions import JSONDecodeError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from typing import Dict, Any, Callable, Generator

logger = logging.getLogger(__name__)

USER_AGENT = "nva-aws-cli-tools (+https://github.com/BIBSYSDEV/nva-aws-cli-tools)"


class SearchApiService:
    NEXT_PAGE_FIELD = "nextSearchAfterResults"

    def __init__(self, session: boto3.Session) -> None:
        self.session = session
        self.ssm = self.session.client("ssm")
        self.api_domain = self._get_system_parameter("/NVA/ApiDomain")

    def _get_system_parameter(self, name: str) -> str:
        response = self.ssm.get_parameter(Name=name)
        return response["Parameter"]["Value"]

    def get_uri(self, type: str) -> str:
        return f"https://{self.api_domain}/search/{type}"

    def find_by_handle(self, handle_value: str) -> list:
        hits = list(
            self.resource_search(
                {"aggregation": "none", "handle": handle_value}, page_size=10
            )
        )
        return [hit for hit in hits if self._hit_contains_handle(hit, handle_value)]

    def _hit_contains_handle(self, hit: dict, handle_value: str) -> bool:
        handles = hit.get("otherIdentifiers", {}).get("handle", [])
        return any(handle_url.endswith(f"/{handle_value}") for handle_url in handles)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(
            (requests.exceptions.RequestException, requests.exceptions.HTTPError)
        ),
        reraise=True,
    )
    def _make_search_request(
        self, url: str, headers: Dict[str, str], params: Dict[str, Any]
    ) -> requests.Response:
        logger.debug(f"URL: {url}")
        logger.debug(f"Params: {params}")
        logger.debug(f"Headers: {headers}")

        response = requests.get(url, headers=headers, params=params, timeout=30)

        logger.debug(f"Status: {response.status_code}")
        logger.debug(f"Full URL: {response.url}")

        if response.status_code >= 500:
            logger.error(f"Server error {response.status_code}, will retry...")
            response.raise_for_status()

        return response

    def resource_search(
        self,
        query_parameters: Dict[str, Any],
        page_size: int = 100,
        api_version: str = "2024-12-01",
        on_total_hits: Callable[[int], None] | None = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Search resources with automatic pagination using search-after.

        Pagination follows the ``nextSearchAfterResults`` link returned in the
        response body instead of an incrementing ``from`` offset. This avoids the
        offset window limit and lets us page through arbitrarily large result sets.

        Args:
            query_parameters: Dictionary of query parameters (without pagination keys)
            page_size: Number of results per page (default: 100)
            api_version: API version to use in Accept header (default: 2024-12-01)
            on_total_hits: Optional callback invoked once with ``totalHits`` from
                the first page, e.g. to size a progress bar

        Yields:
            Individual hits from the search results
        """
        headers = {
            "Accept": f"application/json; version={api_version}",
            "User-Agent": USER_AGENT,
        }
        url = self.get_uri("resources")
        params = {
            **query_parameters,
            "results": page_size,
        }
        total_reported = False

        while url:
            response_data = self._fetch_search_page(url, headers, params)
            if response_data is None:
                break

            if on_total_hits is not None and not total_reported:
                on_total_hits(response_data.get("totalHits", 0))
                total_reported = True

            hits = response_data.get("hits", [])
            if not hits:
                break

            yield from hits

            url = response_data.get(self.NEXT_PAGE_FIELD)
            params = {}

    def _fetch_search_page(
        self,
        url: str,
        headers: Dict[str, str],
        params: Dict[str, Any],
    ) -> Dict[str, Any] | None:
        try:
            response = self._make_search_request(url, headers, params)
        except requests.exceptions.HTTPError as e:
            logger.error(
                f"Failed to search after retries. Status: {e.response.status_code}",
            )
            if e.response.status_code >= 400:
                try:
                    error_detail = e.response.json()
                    logger.error(f"Error detail: {error_detail}")
                except ValueError, JSONDecodeError:
                    logger.error(f"Error detail: {e.response.text}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error after retries: {e}")
            return None

        if response.status_code != 200:
            logger.error(f"Failed to search. {response.status_code}: {response.json()}")
            return None

        return response.json()
