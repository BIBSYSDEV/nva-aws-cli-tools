import boto3
import json
import logging
import requests
from datetime import datetime, timedelta
from requests.exceptions import JSONDecodeError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from typing import Dict, Any, Generator, Optional

logger = logging.getLogger(__name__)


class SearchApiService:
    def __init__(self, profile: Optional[str]) -> None:
        self.session = boto3.Session(profile_name=profile)
        self.ssm = self.session.client("ssm")
        self.secretsmanager = self.session.client("secretsmanager")
        self.api_domain = self._get_system_parameter("/NVA/ApiDomain")
        self._cognito_uri: Optional[str] = None
        self._client_credentials: Optional[Dict[str, str]] = None
        self._token: Optional[str] = None
        self._token_expiry_time: datetime = datetime.now()

    def _get_system_parameter(self, name: str) -> str:
        response = self.ssm.get_parameter(Name=name)
        return response["Parameter"]["Value"]

    def _get_secret(self, name: str) -> Dict[str, str]:
        response = self.secretsmanager.get_secret_value(SecretId=name)
        return json.loads(response["SecretString"])

    def _get_cognito_token(self) -> str:
        url = f"{self._cognito_uri}/oauth2/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_credentials["backendClientId"],
            "client_secret": self._client_credentials["backendClientSecret"],
        }
        response = requests.post(url, headers=headers, data=data)
        response_json = response.json()
        self._token_expiry_time = datetime.now() + timedelta(seconds=response_json["expires_in"])
        return response_json["access_token"]

    def _is_token_expired(self) -> bool:
        return datetime.now() > self._token_expiry_time - timedelta(seconds=30)

    def _get_token(self) -> str:
        if not self._cognito_uri:
            self._cognito_uri = self._get_system_parameter("/NVA/CognitoUri")
            self._client_credentials = self._get_secret("BackendCognitoClientCredentials")
        if not self._token or self._is_token_expired():
            self._token = self._get_cognito_token()
        return self._token

    def get_uri(self, type: str) -> str:
        return f"https://{self.api_domain}/search/{type}"

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
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Search resources with automatic pagination.

        Args:
            query_parameters: Dictionary of query parameters (without 'from' and 'results')
            page_size: Number of results per page (default: 100)
            api_version: API version to use in Accept header (default: 2024-12-01)

        Yields:
            Individual hits from the search results
        """
        url = self.get_uri("resources")
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
                response = self._make_search_request(url, headers, params)
            except requests.exceptions.HTTPError as e:
                logger.error(
                    f"Failed to search after retries. Status: {e.response.status_code}",
                )
                if e.response.status_code >= 400:
                    try:
                        error_detail = e.response.json()
                        logger.error(f"Error detail: {error_detail}")
                    except (ValueError, JSONDecodeError):
                        logger.error(f"Error detail: {e.response.text}")
                break
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error after retries: {e}")
                break

            if response.status_code != 200:
                logger.error(
                    f"Failed to search. {response.status_code}: {response.json()}"
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

    def import_candidates_search(
        self,
        query_parameters: Dict[str, Any],
        page_size: int = 100,
    ) -> Generator[Dict[str, Any], None, None]:
        url = self.get_uri("customer/import-candidates")
        offset = 0

        while True:
            params = {
                **query_parameters,
                "from": offset,
                "size": page_size,
            }
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self._get_token()}",
            }

            try:
                response = self._make_search_request(url, headers, params)
            except requests.exceptions.HTTPError as e:
                logger.error(
                    f"Failed to search after retries. Status: {e.response.status_code}",
                )
                if e.response.status_code >= 400:
                    try:
                        logger.error(f"Error detail: {e.response.json()}")
                    except (ValueError, JSONDecodeError):
                        logger.error(f"Error detail: {e.response.text}")
                break
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error after retries: {e}")
                break

            if response.status_code != 200:
                logger.error(
                    f"Failed to search. {response.status_code}: {response.json()}"
                )
                break

            response_data = response.json()
            hits = response_data.get("hits", [])

            if not hits:
                break

            for hit in hits:
                yield hit

            total_hits = response_data.get("totalHits", 0)
            offset += len(hits)

            if offset >= total_hits:
                break
