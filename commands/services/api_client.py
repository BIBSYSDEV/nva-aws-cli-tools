import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import cached_property

import boto3
import requests

CREDENTIALS_SECRET_NAME = "BackendCognitoClientCredentials"
TOKEN_REFRESH_BUFFER_SECONDS = 30


@dataclass
class ApiClient:
    session: boto3.Session
    _token: str | None = field(default=None, init=False, repr=False)
    _token_expires_at: datetime | None = field(default=None, init=False, repr=False)

    @cached_property
    def _parameters(self) -> dict[str, str]:
        response = self.session.client("ssm").get_parameters(
            Names=["/NVA/ApiDomain", "/NVA/CognitoUri"]
        )
        return {
            parameter["Name"]: parameter["Value"]
            for parameter in response["Parameters"]
        }

    @cached_property
    def _credentials(self) -> dict:
        response = self.session.client("secretsmanager").get_secret_value(
            SecretId=CREDENTIALS_SECRET_NAME
        )
        return json.loads(response["SecretString"])

    @property
    def api_domain(self) -> str:
        return self._parameters["/NVA/ApiDomain"]

    @property
    def cognito_uri(self) -> str:
        return self._parameters["/NVA/CognitoUri"]

    def auth_header(self) -> dict[str, str]:
        if self._token is None or self._is_token_expired():
            self._refresh_token()
        assert self._token is not None
        return {"Authorization": f"Bearer {self._token}"}

    def _refresh_token(self) -> None:
        response = requests.post(
            f"{self.cognito_uri}/oauth2/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": self._credentials["backendClientId"],
                "client_secret": self._credentials["backendClientSecret"],
            },
        )
        response.raise_for_status()
        body = response.json()
        self._token = body["access_token"]
        self._token_expires_at = datetime.now() + timedelta(seconds=body["expires_in"])

    def _is_token_expired(self) -> bool:
        if self._token_expires_at is None:
            return True
        return datetime.now() > self._token_expires_at - timedelta(
            seconds=TOKEN_REFRESH_BUFFER_SECONDS
        )
