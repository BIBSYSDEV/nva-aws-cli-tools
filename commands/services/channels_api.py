import requests

from commands.services.api_client import ApiClient

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


class ChannelNotFoundError(Exception):
    pass


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

    def create_publisher(
        self,
        name: str,
        isbn_prefix: str | None = None,
    ) -> dict:
        body = _drop_none({"name": name, "isbnPrefix": isbn_prefix})
        return self._post(KIND_PUBLISHER, body)

    def create_serial_publication(
        self,
        name: str,
        serial_type: str,
        print_issn: str | None = None,
        online_issn: str | None = None,
    ) -> dict:
        if serial_type not in VALID_SERIAL_TYPES:
            raise ValueError(f"serial_type must be one of {sorted(VALID_SERIAL_TYPES)}")
        body = _drop_none(
            {
                "name": name,
                "type": serial_type,
                "printIssn": print_issn,
                "onlineIssn": online_issn,
            }
        )
        return self._post(KIND_SERIAL, body)

    def create_journal(
        self,
        name: str,
        print_issn: str | None = None,
        online_issn: str | None = None,
    ) -> dict:
        body = _drop_none(
            {
                "name": name,
                "printIssn": print_issn,
                "onlineIssn": online_issn,
            }
        )
        return self._post(KIND_JOURNAL, body)

    def create_series(
        self,
        name: str,
        print_issn: str | None = None,
        online_issn: str | None = None,
    ) -> dict:
        body = _drop_none(
            {
                "name": name,
                "printIssn": print_issn,
                "onlineIssn": online_issn,
            }
        )
        return self._post(KIND_SERIES, body)

    def update_publisher(
        self,
        identifier: str,
        name: str | None = None,
        isbn: str | None = None,
    ) -> dict:
        # Backend uses 'isbn' on update but 'isbnPrefix' on create — see UpdatePublisherRequest.java
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
            f"https://{self.client.api_domain}/{CHANNEL_BASE_PATH}/"
            f"{DELETE_PATH_SEGMENT}/{identifier}"
        )
        response = requests.delete(url, headers=self.client.auth_header())
        if response.status_code == 404:
            raise ChannelNotFoundError(f"channel {identifier} not found")
        response.raise_for_status()

    def _channel_url(self, kind: str) -> str:
        if kind not in VALID_KINDS:
            raise ValueError(f"Unknown channel kind: {kind}")
        return f"https://{self.client.api_domain}/{CHANNEL_BASE_PATH}/{kind}"

    def _post(self, kind: str, body: dict) -> dict:
        headers = self.client.auth_header() | {"Content-Type": CONTENT_TYPE_LD_JSON}
        response = requests.post(self._channel_url(kind), headers=headers, json=body)
        response.raise_for_status()
        if response.text:
            return response.json()
        return {"location": response.headers.get("Location")}

    def _put(self, kind: str, identifier: str, body: dict) -> dict:
        url = f"{self._channel_url(kind)}/{identifier}"
        headers = self.client.auth_header() | {"Content-Type": CONTENT_TYPE_JSON}
        response = requests.put(url, headers=headers, json=body)
        response.raise_for_status()
        if response.status_code == 202:
            return {"status": "accepted"}
        if response.text:
            return response.json()
        return {}


def _drop_none(body: dict) -> dict:
    return {key: value for key, value in body.items() if value is not None}
