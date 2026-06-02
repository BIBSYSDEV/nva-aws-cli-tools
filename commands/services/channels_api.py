from dataclasses import dataclass

import requests

from commands.services.api_client import ApiClient

CHANNEL_BASE_PATH = "publication-channels-v2"
DELETE_PATH_SEGMENT = "channel"
REQUEST_TIMEOUT_SECONDS = 30
HTTP_ERROR_BODY_SNIPPET_LENGTH = 500

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

HTTP_NOT_FOUND = 404


class ChannelApiError(Exception):
    pass


class ChannelNotFoundError(ChannelApiError):
    pass


@dataclass(kw_only=True)
class PublisherCreate:
    name: str
    isbn_prefix: str | None = None

    def to_body(self) -> dict:
        return _drop_none({"name": self.name, "isbnPrefix": self.isbn_prefix})


@dataclass(kw_only=True)
class SerialCreate:
    name: str
    serial_type: str | None = None
    print_issn: str | None = None
    online_issn: str | None = None

    def __post_init__(self) -> None:
        if self.serial_type is not None and self.serial_type not in VALID_SERIAL_TYPES:
            raise ValueError(f"serial_type must be one of {sorted(VALID_SERIAL_TYPES)}")

    def to_body(self) -> dict:
        return _drop_none(
            {
                "name": self.name,
                "type": self.serial_type,
                "printIssn": self.print_issn,
                "onlineIssn": self.online_issn,
            }
        )


@dataclass(kw_only=True)
class PublisherUpdate:
    name: str | None = None
    isbn: str | None = None

    def to_body(self) -> dict:
        # Backend uses 'isbn' on update but 'isbnPrefix' on create — see UpdatePublisherRequest.java
        return _drop_none(
            {
                "type": UPDATE_PUBLISHER_REQUEST_TYPE,
                "name": self.name,
                "isbn": self.isbn,
            }
        )


@dataclass(kw_only=True)
class SerialUpdate:
    name: str | None = None
    print_issn: str | None = None
    online_issn: str | None = None

    def to_body(self) -> dict:
        return _drop_none(
            {
                "type": UPDATE_SERIAL_PUBLICATION_REQUEST_TYPE,
                "name": self.name,
                "printIssn": self.print_issn,
                "onlineIssn": self.online_issn,
            }
        )


class ChannelsApiService:
    def __init__(self, client: ApiClient):
        self.client = client

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
        response = self._request("GET", self._channel_url(kind), params=params)
        return response.json()

    def fetch(self, kind: str, identifier: str, year: int | None = None) -> dict:
        url = f"{self._channel_url(kind)}/{identifier}"
        if year is not None:
            url = f"{url}/{year}"
        response = self._request(
            "GET", url, not_found_message=f"{kind}/{identifier} not found"
        )
        return response.json()

    def fetch_auto(self, identifier: str, year: int | None = None) -> tuple[dict, str]:
        for kind in (KIND_SERIAL, KIND_PUBLISHER):
            try:
                channel = self.fetch(kind, identifier, year)
                return channel, kind
            except ChannelNotFoundError:
                continue
        raise ChannelNotFoundError(
            f"No channel with identifier {identifier} found in serial-publication or publisher"
        )

    def create_publisher(self, request: PublisherCreate) -> dict:
        return self._post(KIND_PUBLISHER, request.to_body())

    def create_serial_publication(self, request: SerialCreate) -> dict:
        if request.serial_type is None:
            raise ValueError("serial_type is required for /serial-publication")
        return self._post(KIND_SERIAL, request.to_body())

    def create_journal(self, request: SerialCreate) -> dict:
        return self._post(KIND_JOURNAL, request.to_body())

    def create_series(self, request: SerialCreate) -> dict:
        return self._post(KIND_SERIES, request.to_body())

    def update_publisher(self, identifier: str, request: PublisherUpdate) -> None:
        self._put(KIND_PUBLISHER, identifier, request.to_body())

    def update_serial_publication(self, identifier: str, request: SerialUpdate) -> None:
        self._put(KIND_SERIAL, identifier, request.to_body())

    def delete_channel(self, identifier: str) -> None:
        url = (
            f"https://{self.client.api_domain}/{CHANNEL_BASE_PATH}/"
            f"{DELETE_PATH_SEGMENT}/{identifier}"
        )
        self._request(
            "DELETE",
            url,
            headers=self.client.auth_header(),
            not_found_message=f"channel {identifier} not found",
        )

    def _channel_url(self, kind: str) -> str:
        if kind not in VALID_KINDS:
            raise ValueError(f"Unknown channel kind: {kind}")
        return f"https://{self.client.api_domain}/{CHANNEL_BASE_PATH}/{kind}"

    def _post(self, kind: str, body: dict) -> dict:
        headers = self.client.auth_header() | {"Content-Type": CONTENT_TYPE_LD_JSON}
        response = self._request(
            "POST", self._channel_url(kind), headers=headers, json=body
        )
        if response.text:
            return response.json()
        return {"location": response.headers.get("Location")}

    def _put(self, kind: str, identifier: str, body: dict) -> None:
        url = f"{self._channel_url(kind)}/{identifier}"
        headers = self.client.auth_header() | {"Content-Type": CONTENT_TYPE_JSON}
        self._request("PUT", url, headers=headers, json=body)

    def _request(
        self,
        method: str,
        url: str,
        *,
        not_found_message: str | None = None,
        **kwargs,
    ) -> requests.Response:
        try:
            response = requests.request(
                method, url, timeout=REQUEST_TIMEOUT_SECONDS, **kwargs
            )
        except requests.RequestException as exc:
            raise ChannelApiError(
                f"Network error contacting channel API: {exc}"
            ) from exc

        if response.status_code == HTTP_NOT_FOUND and not_found_message is not None:
            raise ChannelNotFoundError(not_found_message)
        if not response.ok:
            raise ChannelApiError(_format_http_error_message(response))
        return response


def _format_http_error_message(response: requests.Response) -> str:
    snippet = (
        response.text[:HTTP_ERROR_BODY_SNIPPET_LENGTH].strip() if response.text else ""
    )
    base = f"API error {response.status_code} from {response.url}"
    return f"{base}\n  {snippet}" if snippet else base


def _drop_none(body: dict) -> dict:
    return {key: value for key, value in body.items() if value is not None}
