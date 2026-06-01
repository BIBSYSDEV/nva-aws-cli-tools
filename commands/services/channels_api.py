import json
from datetime import datetime, timedelta

import boto3
import requests

CHANNEL_BASE_PATH = "publication-channels-v2"
DELETE_PATH_SEGMENT = "channel"

KIND_PUBLISHER = "publisher"
KIND_SERIAL = "serial-publication"
KIND_JOURNAL = "journal"
KIND_SERIES = "series"

VALID_KINDS = {KIND_PUBLISHER, KIND_SERIAL, KIND_JOURNAL, KIND_SERIES}

SERIAL_TYPE_JOURNAL = "Journal"
SERIAL_TYPE_SERIES = "Series"
VALID_SERIAL_TYPES = {SERIAL_TYPE_JOURNAL, SERIAL_TYPE_SERIES}

UPDATE_PUBLISHER_REQUEST_TYPE = "UpdatePublisherRequest"
UPDATE_SERIAL_PUBLICATION_REQUEST_TYPE = "UpdateSerialPublicationRequest"

CONTENT_TYPE_LD_JSON = "application/ld+json"
CONTENT_TYPE_JSON = "application/json"

TOKEN_REFRESH_MARGIN_SECONDS = 30


class ChannelNotFoundError(Exception):
    pass


class ChannelsApiService:
    def __init__(self, profile: str | None):
        self.session = boto3.Session(profile_name=profile)
        self.ssm = self.session.client("ssm")
        self.secretsmanager = self.session.client("secretsmanager")
        self.api_domain = self._get_system_parameter("/NVA/ApiDomain")
        self.cognito_uri = self._get_system_parameter("/NVA/CognitoUri")
        self.client_credentials = self._get_secret("BackendCognitoClientCredentials")
        self.token: str | None = None
        self.token_expiry_time = datetime.min

    def search(
        self,
        kind: str,
        query: str,
        year: int | None = None,
        offset: int = 0,
        size: int = 10,
    ) -> dict:
        params = {"query": query, "offset": offset, "size": size}
        if year is not None:
            params["year"] = year
        response = requests.get(self._channel_url(kind), params=params)
        response.raise_for_status()
        return response.json()

    def fetch(self, kind: str, identifier: str, year: int | None = None) -> dict:
        url = f"{self._channel_url(kind)}/{identifier}"
        if year is not None:
            url = f"{url}/{year}"
        response = requests.get(url)
        if response.status_code == 404:
            raise ChannelNotFoundError(f"{kind}/{identifier} not found")
        response.raise_for_status()
        return response.json()

    def fetch_auto(self, identifier: str, year: int | None = None) -> dict:
        last_http_error: requests.HTTPError | None = None
        for kind in (KIND_SERIAL, KIND_PUBLISHER):
            try:
                channel = self.fetch(kind, identifier, year)
                channel.setdefault("_resolvedKind", kind)
                return channel
            except ChannelNotFoundError:
                continue
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else 0
                if 500 <= status < 600:
                    last_http_error = exc
                    continue
                raise
        if last_http_error is not None:
            raise last_http_error
        raise ChannelNotFoundError(
            f"No channel with identifier {identifier} found in serial-publication or publisher"
        )

    def create_publisher(
        self,
        name: str,
        isbn_prefix: str | None = None,
        homepage: str | None = None,
    ) -> dict:
        body = _drop_none(
            {"name": name, "isbnPrefix": isbn_prefix, "homepage": homepage}
        )
        return self._post(KIND_PUBLISHER, body)

    def create_serial_publication(
        self,
        name: str,
        serial_type: str,
        print_issn: str | None = None,
        online_issn: str | None = None,
        homepage: str | None = None,
    ) -> dict:
        if serial_type not in VALID_SERIAL_TYPES:
            raise ValueError(f"serial_type must be one of {sorted(VALID_SERIAL_TYPES)}")
        body = _drop_none(
            {
                "name": name,
                "type": serial_type,
                "printIssn": print_issn,
                "onlineIssn": online_issn,
                "homepage": homepage,
            }
        )
        return self._post(KIND_SERIAL, body)

    def create_journal(
        self,
        name: str,
        print_issn: str | None = None,
        online_issn: str | None = None,
        homepage: str | None = None,
    ) -> dict:
        body = _drop_none(
            {
                "name": name,
                "printIssn": print_issn,
                "onlineIssn": online_issn,
                "homepage": homepage,
            }
        )
        return self._post(KIND_JOURNAL, body)

    def create_series(
        self,
        name: str,
        print_issn: str | None = None,
        online_issn: str | None = None,
        homepage: str | None = None,
    ) -> dict:
        body = _drop_none(
            {
                "name": name,
                "printIssn": print_issn,
                "onlineIssn": online_issn,
                "homepage": homepage,
            }
        )
        return self._post(KIND_SERIES, body)

    def update_publisher(
        self,
        identifier: str,
        name: str | None = None,
        isbn: str | None = None,
    ) -> dict:
        body = _drop_none(
            {"type": UPDATE_PUBLISHER_REQUEST_TYPE, "name": name, "isbn": isbn}
        )
        return self._put(KIND_PUBLISHER, identifier, body)

    def update_serial_publication(
        self,
        identifier: str,
        name: str | None = None,
        print_issn: str | None = None,
        online_issn: str | None = None,
    ) -> dict:
        body = _drop_none(
            {
                "type": UPDATE_SERIAL_PUBLICATION_REQUEST_TYPE,
                "name": name,
                "printIssn": print_issn,
                "onlineIssn": online_issn,
            }
        )
        return self._put(KIND_SERIAL, identifier, body)

    def delete_channel(self, identifier: str) -> None:
        url = (
            f"https://{self.api_domain}/{CHANNEL_BASE_PATH}/"
            f"{DELETE_PATH_SEGMENT}/{identifier}"
        )
        response = requests.delete(url, headers=self._auth_headers())
        if response.status_code == 404:
            raise ChannelNotFoundError(f"channel {identifier} not found")
        response.raise_for_status()

    def _channel_url(self, kind: str) -> str:
        if kind not in VALID_KINDS:
            raise ValueError(f"Unknown channel kind: {kind}")
        return f"https://{self.api_domain}/{CHANNEL_BASE_PATH}/{kind}"

    def _auth_headers(self) -> dict:
        return {"Authorization": f"Bearer {self._get_token()}"}

    def _post(self, kind: str, body: dict) -> dict:
        headers = self._auth_headers() | {"Content-Type": CONTENT_TYPE_LD_JSON}
        response = requests.post(self._channel_url(kind), headers=headers, json=body)
        response.raise_for_status()
        if response.text:
            return response.json()
        return {"location": response.headers.get("Location")}

    def _put(self, kind: str, identifier: str, body: dict) -> dict:
        url = f"{self._channel_url(kind)}/{identifier}"
        headers = self._auth_headers() | {"Content-Type": CONTENT_TYPE_JSON}
        response = requests.put(url, headers=headers, json=body)
        response.raise_for_status()
        if response.status_code == 202:
            return {"status": "accepted"}
        if response.text:
            return response.json()
        return {}

    def _get_system_parameter(self, name: str) -> str:
        response = self.ssm.get_parameter(Name=name)
        return response["Parameter"]["Value"]

    def _get_secret(self, name: str) -> dict:
        response = self.secretsmanager.get_secret_value(SecretId=name)
        return json.loads(response["SecretString"])

    def _get_cognito_token(self) -> str:
        url = f"{self.cognito_uri}/oauth2/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_credentials["backendClientId"],
            "client_secret": self.client_credentials["backendClientSecret"],
        }
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        response_json = response.json()
        self.token_expiry_time = datetime.now() + timedelta(
            seconds=response_json["expires_in"]
        )
        return response_json["access_token"]

    def _is_token_expired(self) -> bool:
        return datetime.now() > self.token_expiry_time - timedelta(
            seconds=TOKEN_REFRESH_MARGIN_SECONDS
        )

    def _get_token(self) -> str:
        if self.token is None or self._is_token_expired():
            self.token = self._get_cognito_token()
        return self.token


def _drop_none(body: dict) -> dict:
    return {key: value for key, value in body.items() if value is not None}
