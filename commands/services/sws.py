import boto3
import logging
import requests
import json
from typing import Optional

logger = logging.getLogger(__name__)


class SwsService:
    TOKEN_ENDPOINT_PROD = "https://sws-auth.auth.eu-west-1.amazoncognito.com/token"
    TOKEN_ENDPOINT_NON_PROD = "https://sws-auth-dev.auth.eu-west-1.amazoncognito.com/token"
    API_ENDPOINT_PROD = "https://api.sws.aws.sikt.no"
    API_ENDPOINT_NON_PROD = "https://api.dev.sws.aws.sikt.no"
    SECRET_NAME = "SearchInfrastructureCredentials"

    def __init__(self, profile: str):
        self.profile = profile
        session = (
            boto3.Session(profile_name=self.profile)
            if self.profile
            else boto3.Session()
        )
        self.secretsmanager = session.client("secretsmanager")

        if self.profile and "prod" in self.profile.lower():
            self.token_endpoint = self.TOKEN_ENDPOINT_PROD
            self.api_endpoint = self.API_ENDPOINT_PROD
        else:
            self.token_endpoint = self.TOKEN_ENDPOINT_NON_PROD
            self.api_endpoint = self.API_ENDPOINT_NON_PROD

        self._access_token: Optional[str] = None

    def _get_secret(self, name: str) -> dict:
        response = self.secretsmanager.get_secret_value(SecretId=name)
        secret_string = response["SecretString"]
        return json.loads(secret_string)

    def _get_access_token(self) -> str:
        if self._access_token:
            return self._access_token

        credentials = self._get_secret(self.SECRET_NAME)
        client_id = credentials.get("username")
        client_secret = credentials.get("password")

        if not client_id or not client_secret:
            raise ValueError(
                f"Missing username or password in {self.SECRET_NAME}"
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

        if not response.ok:
            logger.error(f"Failed to get access token: {response.text}")
            raise RuntimeError(f"OAuth2 token request failed: {response.status_code}")

        token_data = response.json()
        self._access_token = token_data["access_token"]
        return self._access_token

    def get_mappings(self, index: str) -> Optional[dict]:
        token = self._get_access_token()
        url = f"{self.api_endpoint}/{index}/_mapping"

        response = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )

        if not response.ok:
            logger.error(f"Failed to get mappings for index '{index}': {response.text}")
            return None

        return response.json()
