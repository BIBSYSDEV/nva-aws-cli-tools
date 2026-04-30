import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import boto3
import requests

CREDENTIALS_SECRET_NAME = "BackendCognitoClientCredentials"
TOKEN_REFRESH_BUFFER_SECONDS = 30


@dataclass
class ApiClient:
    """Authenticated NVA API client. SSM/Secrets Manager/Cognito calls happen
    lazily on first access, so tests can construct the client without I/O."""

    session: boto3.Session
    _api_domain: str | None = field(default=None, repr=False)
    _cognito_uri: str | None = field(default=None, repr=False)
    _credentials: dict | None = field(default=None, repr=False)
    _token: str | None = field(default=None, repr=False)
    _token_expires_at: datetime | None = field(default=None, repr=False)

    @property
    def api_domain(self) -> str:
        if self._api_domain is None:
            self._load_parameters()
        return self._api_domain

    @property
    def cognito_uri(self) -> str:
        if self._cognito_uri is None:
            self._load_parameters()
        return self._cognito_uri

    def auth_header(self) -> dict[str, str]:
        if self._token is None or self._is_token_expired():
            self._refresh_token()
        return {"Authorization": f"Bearer {self._token}"}

    def _load_parameters(self) -> None:
        ssm = self.session.client("ssm")
        response = ssm.get_parameters(Names=["/NVA/ApiDomain", "/NVA/CognitoUri"])
        params = {parameter["Name"]: parameter["Value"] for parameter in response["Parameters"]}
        self._api_domain = params["/NVA/ApiDomain"]
        self._cognito_uri = params["/NVA/CognitoUri"]

    def _refresh_token(self) -> None:
        if self._credentials is None:
            self._credentials = self._load_credentials()
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

    def _load_credentials(self) -> dict:
        secrets = self.session.client("secretsmanager")
        response = secrets.get_secret_value(SecretId=CREDENTIALS_SECRET_NAME)
        return json.loads(response["SecretString"])

    def _is_token_expired(self) -> bool:
        if self._token_expires_at is None:
            return True
        return datetime.now() > self._token_expires_at - timedelta(seconds=TOKEN_REFRESH_BUFFER_SECONDS)
