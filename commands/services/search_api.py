import boto3
import json
import logging
import requests
from dataclasses import dataclass
from tenacity import (
    retry,
    stop_after_attempt,
    wait_none,
    retry_if_exception_type,
)
from typing import Dict, Any, Callable, Generator
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

logger = logging.getLogger(__name__)

USER_AGENT = "nva-aws-cli-tools (+https://github.com/BIBSYSDEV/nva-aws-cli-tools)"

START_PAGE_SIZE = 50
MIN_PAGE_SIZE = 1
RAMP_UP_STEP = 50
RAMP_UP_AFTER_SUCCESSES = 2

NETWORK_RETRY_ATTEMPTS = 3

RESULTS_PARAM = "results"
PAGE_SIZE_PARAMS = ("results", "size")
SEARCH_AFTER_PARAMS = ("search_after", "searchAfter")


@dataclass
class SearchPageOutcome:
    data: Dict[str, Any] | None = None
    should_reduce_page_size: bool = False
    status_code: int | None = None
    error_body: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.data is not None


class AdaptivePageSize:
    """Tunes the page size AIMD-style: shrink fast on failure, grow slowly on success.

    On a failing page the size is halved toward ``minimum`` so we can isolate a
    record the server chokes on. After a run of successful pages the size grows
    additively back toward ``maximum``.
    """

    def __init__(
        self,
        maximum: int,
        minimum: int = MIN_PAGE_SIZE,
        start: int = START_PAGE_SIZE,
        step: int = RAMP_UP_STEP,
        successes_before_growth: int = RAMP_UP_AFTER_SUCCESSES,
    ) -> None:
        self._maximum = maximum
        self._minimum = min(minimum, maximum)
        self._step = step
        self._successes_before_growth = successes_before_growth
        self._current = self._clamp(start)
        self._consecutive_successes = 0

    @property
    def current(self) -> int:
        return self._current

    def can_shrink(self) -> bool:
        return self._current > self._minimum

    def shrink(self) -> int:
        self._current = max(self._minimum, self._current // 2)
        self._consecutive_successes = 0
        return self._current

    def register_success(self) -> None:
        self._consecutive_successes += 1
        if self._consecutive_successes >= self._successes_before_growth:
            self._current = min(self._maximum, self._current + self._step)
            self._consecutive_successes = 0

    def _clamp(self, value: int) -> int:
        return max(self._minimum, min(self._maximum, value))


def _url_with_page_size(url: str, page_size: int) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query[RESULTS_PARAM] = str(page_size)
    for param in PAGE_SIZE_PARAMS:
        if param in query:
            query[param] = str(page_size)
    return urlunparse(parsed._replace(query=urlencode(query)))


def _search_after_cursor(url: str) -> str | None:
    query = dict(parse_qsl(urlparse(url).query, keep_blank_values=True))
    for param in SEARCH_AFTER_PARAMS:
        if param in query:
            return query[param]
    return None


def _without_page_size_params(query_parameters: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in query_parameters.items()
        if key not in PAGE_SIZE_PARAMS
    }


def _accept_header(api_version: str | None) -> str:
    if api_version:
        return f"application/json; version={api_version}"
    return "application/json"


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
        stop=stop_after_attempt(NETWORK_RETRY_ATTEMPTS),
        wait=wait_none(),
        retry=retry_if_exception_type(
            (requests.exceptions.ConnectionError, requests.exceptions.Timeout)
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
            logger.debug(f"Server error {response.status_code}; will reduce page size")
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

        The page size adapts as we go: it starts low, ramps up toward ``page_size``
        while pages succeed, and backs off (down to a single record) when the
        server fails a page, so a large page size cannot stall the whole export.

        Args:
            query_parameters: Dictionary of query parameters (without pagination keys)
            page_size: Maximum number of results per page; the client ramps up
                toward this and backs off below it on failures (default: 100)
            api_version: API version to use in Accept header (default: 2024-12-01)
            on_total_hits: Optional callback invoked once with ``totalHits`` from
                the first page, e.g. to size a progress bar

        Yields:
            Individual hits from the search results
        """
        headers = {
            "Accept": _accept_header(api_version),
            "User-Agent": USER_AGENT,
        }
        page_size_control = AdaptivePageSize(maximum=page_size)
        url = self.get_uri("resources")
        base_params = _without_page_size_params(query_parameters)
        params = {**base_params, RESULTS_PARAM: page_size_control.current}
        total_reported = False
        last_identifier = None

        while url:
            response_data = self._fetch_page_with_backoff(
                url, headers, params, page_size_control, last_identifier
            )
            if response_data is None:
                break

            if on_total_hits is not None and not total_reported:
                on_total_hits(response_data.get("totalHits", 0))
                total_reported = True

            hits = response_data.get("hits", [])
            if not hits:
                break

            yield from hits
            last_identifier = hits[-1].get("identifier")

            next_url = response_data.get(self.NEXT_PAGE_FIELD)
            if next_url:
                url = _url_with_page_size(next_url, page_size_control.current)
                params = {}
            else:
                url = None

    def _fetch_page_with_backoff(
        self,
        url: str,
        headers: Dict[str, str],
        params: Dict[str, Any],
        page_size_control: AdaptivePageSize,
        previous_identifier: str | None,
    ) -> Dict[str, Any] | None:
        while True:
            outcome = self._fetch_search_page(url, headers, params)
            if outcome.succeeded:
                page_size_control.register_success()
                return outcome.data

            if not outcome.should_reduce_page_size:
                self._log_terminal_failure(outcome)
                return None

            if not page_size_control.can_shrink():
                self._log_poison_diagnostics(
                    url, outcome, page_size_control.current, previous_identifier
                )
                return None

            failing_size = page_size_control.current
            reduced = page_size_control.shrink()
            logger.warning(
                "Search failed with status %s at page size %d; "
                "backing off to %d and retrying",
                outcome.status_code,
                failing_size,
                reduced,
            )
            url, params = self._apply_page_size(url, params, reduced)

    def _apply_page_size(
        self, url: str, params: Dict[str, Any], page_size: int
    ) -> tuple[str, Dict[str, Any]]:
        carried_keys = [param for param in PAGE_SIZE_PARAMS if param in params]
        if not carried_keys:
            return _url_with_page_size(url, page_size), params
        updated = {**params, RESULTS_PARAM: page_size}
        for param in carried_keys:
            updated[param] = page_size
        return url, updated

    def _log_terminal_failure(self, outcome: SearchPageOutcome) -> None:
        if outcome.status_code is None:
            logger.error(
                "Search failed due to a network error and will not be retried: %s",
                outcome.error_body,
            )
        else:
            logger.error(
                "Search failed with status %s and will not be retried: %s",
                outcome.status_code,
                outcome.error_body,
            )

    def _log_poison_diagnostics(
        self,
        url: str,
        outcome: SearchPageOutcome,
        page_size: int,
        previous_identifier: str | None,
    ) -> None:
        logger.error(
            "Search still failing at page size %d (status %s) after backing off to "
            "the minimum. A single poisoned record right after the current position "
            "is the likely cause.",
            page_size,
            outcome.status_code,
        )
        logger.error("Failing request URL: %s", url)
        cursor = _search_after_cursor(url)
        if cursor is not None:
            logger.error("search-after cursor at failure: %s", cursor)
        if previous_identifier is not None:
            logger.error(
                "Last identifier fetched successfully before failure: %s "
                "(the poisoned record is the next one in sort order)",
                previous_identifier,
            )
        if outcome.error_body:
            logger.error("Response body: %s", outcome.error_body)

    def _fetch_search_page(
        self,
        url: str,
        headers: Dict[str, str],
        params: Dict[str, Any],
    ) -> SearchPageOutcome:
        try:
            response = self._make_search_request(url, headers, params)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            body = self._error_body(e.response)
            logger.debug(
                "Search request failed after retries (status %s): %s", status, body
            )
            return SearchPageOutcome(
                should_reduce_page_size=self._is_server_error(status),
                status_code=status,
                error_body=body,
            )
        except requests.exceptions.RequestException as e:
            logger.debug("Network error after retries: %s", e)
            return SearchPageOutcome(should_reduce_page_size=False, error_body=str(e))

        if response.status_code != 200:
            logger.debug(
                "Search returned status %s: %s", response.status_code, response.text
            )
            return SearchPageOutcome(
                should_reduce_page_size=self._is_server_error(response.status_code),
                status_code=response.status_code,
                error_body=response.text,
            )

        try:
            return SearchPageOutcome(data=response.json())
        except ValueError:
            logger.debug("Search response was not valid JSON: %s", response.text)
            return SearchPageOutcome(
                status_code=response.status_code, error_body=response.text
            )

    @staticmethod
    def _is_server_error(status_code: int | None) -> bool:
        return status_code is not None and status_code >= 500

    @staticmethod
    def _error_body(response: requests.Response | None) -> str | None:
        if response is None:
            return None
        try:
            return json.dumps(response.json())
        except ValueError:
            return response.text
