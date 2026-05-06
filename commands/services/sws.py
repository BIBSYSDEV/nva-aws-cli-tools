import json
import logging
from dataclasses import dataclass, field
from functools import cached_property

import boto3
import requests

logger = logging.getLogger(__name__)

TOKEN_ENDPOINT_PROD = "https://sws-auth.auth.eu-west-1.amazoncognito.com/token"
TOKEN_ENDPOINT_NON_PROD = "https://sws-auth-dev.auth.eu-west-1.amazoncognito.com/token"
API_ENDPOINT_PROD = "https://api.sws.aws.sikt.no"
API_ENDPOINT_NON_PROD = "https://api.dev.sws.aws.sikt.no"
CREDENTIALS_SECRET_NAME = "SearchInfrastructureCredentials"


@dataclass
class SwsClient:
    session: boto3.Session
    profile: str | None
    _token: str | None = field(default=None, init=False, repr=False)

    @property
    def token_endpoint(self) -> str:
        return TOKEN_ENDPOINT_PROD if self._is_prod() else TOKEN_ENDPOINT_NON_PROD

    @property
    def api_endpoint(self) -> str:
        return API_ENDPOINT_PROD if self._is_prod() else API_ENDPOINT_NON_PROD

    def auth_header(self) -> dict[str, str]:
        if self._token is None:
            self._refresh_token()
        assert self._token is not None
        return {"Authorization": f"Bearer {self._token}"}

    @cached_property
    def _credentials(self) -> dict:
        response = self.session.client("secretsmanager").get_secret_value(
            SecretId=CREDENTIALS_SECRET_NAME
        )
        return json.loads(response["SecretString"])

    def _refresh_token(self) -> None:
        client_id = self._credentials.get("username")
        client_secret = self._credentials.get("password")
        if not client_id or not client_secret:
            raise ValueError(
                f"Missing username or password in {CREDENTIALS_SECRET_NAME}"
            )

        response = requests.post(
            self.token_endpoint,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()
        self._token = response.json()["access_token"]

    def _is_prod(self) -> bool:
        return bool(self.profile) and "prod" in self.profile.lower()


def get_mappings(client: SwsClient, index: str) -> dict | None:
    response = requests.get(
        f"{client.api_endpoint}/{index}/_mapping",
        headers={**client.auth_header(), "Content-Type": "application/json"},
    )
    if not response.ok:
        logger.error("Failed to get mappings for index '%s': %s", index, response.text)
        return None
    return response.json()
