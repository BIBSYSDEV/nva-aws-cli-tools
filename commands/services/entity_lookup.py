import logging

import boto3
import requests

from commands.services.api_client import ApiClient
from commands.services.channels_api import KIND_PUBLISHER, ChannelsApiService

logger = logging.getLogger(__name__)

CRISTIN_PERSON_PATH = "cristin/person"
CRISTIN_PROJECT_PATH = "cristin/project"
LOOKUP_TIMEOUT_SECONDS = 15
LABEL_LANGUAGE_PRIORITY = ("nb", "nn", "en")


class EntityResolver:
    """Best-effort lookup of entity display names; never raises, returns None on failure."""

    def __init__(self, session: boto3.Session):
        self._api_client = ApiClient(session=session)
        self._channels = ChannelsApiService(self._api_client)

    def publisher_name(self, identifier: str) -> str | None:
        try:
            return _display_name_publisher(
                self._channels.fetch(KIND_PUBLISHER, identifier)
            )
        except Exception as error:  # noqa: BLE001 - lookup must never block the update
            logger.debug("Publisher lookup failed for %s: %s", identifier, error)
            return None

    def person_name(self, identifier: str) -> str | None:
        return _display_name_person(
            self._get_path(f"{CRISTIN_PERSON_PATH}/{identifier}")
        )

    def project_title(self, identifier: str) -> str | None:
        return _display_name_project(
            self._get_path(f"{CRISTIN_PROJECT_PATH}/{identifier}")
        )

    def organization_label(self, uri: str) -> str | None:
        return _display_name_organization(self._get_url(uri))

    def _get_path(self, path: str) -> dict | None:
        try:
            url = f"https://{self._api_client.api_domain}/{path}"
        except Exception as error:  # noqa: BLE001 - lookup must never block the update
            logger.debug("Could not resolve api domain for %s: %s", path, error)
            return None
        return self._get_url(url)

    def _get_url(self, url: str) -> dict | None:
        try:
            response = requests.get(
                url,
                headers={"Accept": "application/json"},
                timeout=LOOKUP_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.json()
        except Exception as error:  # noqa: BLE001 - lookup must never block the update
            logger.debug("Entity lookup failed for %s: %s", url, error)
            return None


def _display_name_publisher(data: dict | None) -> str | None:
    return _text_or_label_map(data, "name")


def _display_name_person(data: dict | None) -> str | None:
    if not isinstance(data, dict):
        return None
    return _person_name(data)


def _display_name_project(data: dict | None) -> str | None:
    return _text_or_label_map(data, "title")


def _display_name_organization(data: dict | None) -> str | None:
    if not isinstance(data, dict):
        return None
    return _from_label_map(data.get("labels"))


def _text_or_label_map(data: dict | None, key: str) -> str | None:
    if not isinstance(data, dict):
        return None
    value = data.get(key)
    if isinstance(value, str) and value.strip():
        return value
    return _from_label_map(value)


def _from_label_map(labels: object) -> str | None:
    if not isinstance(labels, dict) or not labels:
        return None
    for language in LABEL_LANGUAGE_PRIORITY:
        value = labels.get(language)
        if isinstance(value, str) and value.strip():
            return value
    for value in labels.values():
        if isinstance(value, str) and value.strip():
            return value
    return None


def _person_name(data: dict) -> str | None:
    from_array = _from_names_array(data.get("names"))
    if from_array:
        return from_array
    first = data.get("firstName") or data.get("first_name")
    last = data.get("lastName") or data.get("surname")
    return _join_name(first, last)


def _from_names_array(names: object) -> str | None:
    if not isinstance(names, list):
        return None
    values = {
        entry.get("type"): entry.get("value")
        for entry in names
        if isinstance(entry, dict)
    }
    first = values.get("FirstName") or values.get("PreferredFirstName")
    last = values.get("LastName") or values.get("PreferredLastName")
    return _join_name(first, last)


def _join_name(first: object, last: object) -> str | None:
    parts = [part for part in (first, last) if isinstance(part, str) and part.strip()]
    return " ".join(parts) if parts else None
