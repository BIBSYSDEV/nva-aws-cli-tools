import logging

import boto3
import requests

from commands.services.api_client import ApiClient
from commands.services.channels_api import KIND_PUBLISHER, ChannelsApiService

logger = logging.getLogger(__name__)

CRISTIN_PERSON_PATH = "cristin/person"
CRISTIN_PROJECT_PATH = "cristin/project"
CRISTIN_POSITION_PATH = "cristin/position"
LOOKUP_TIMEOUT_SECONDS = 15
LABEL_LANGUAGE_PRIORITY = ("nb", "nn", "en")
NATIONAL_IDENTIFICATION_NUMBER_TYPE = "NationalIdentificationNumber"
EMPLOYMENT_LINE_INDENT = "  "


class EntityResolver:
    """Best-effort lookup of entity display names; never raises, returns None on failure."""

    def __init__(self, session: boto3.Session):
        self._api_client = ApiClient(session=session)
        self._channels = ChannelsApiService(self._api_client)
        self._position_labels_cache: dict[str, str] | None = None

    def publisher_name(self, identifier: str) -> str | None:
        try:
            return _display_name_publisher(
                self._channels.fetch(KIND_PUBLISHER, identifier)
            )
        except Exception as error:  # noqa: BLE001 - lookup must never block the update
            logger.debug("Publisher lookup failed for %s: %s", identifier, error)
            return None

    def person_label(self, identifier: str) -> str | None:
        data = self._get_path(f"{CRISTIN_PERSON_PATH}/{identifier}", authenticated=True)
        label = _display_label_person(data)
        if label is None:
            return None
        employment_lines = self._employment_lines(data)
        return "\n".join([label, *employment_lines])

    def project_title(self, identifier: str) -> str | None:
        return _display_name_project(
            self._get_path(f"{CRISTIN_PROJECT_PATH}/{identifier}")
        )

    def organization_label(self, uri: str) -> str | None:
        return _display_name_organization(self._get_url(uri))

    def _employment_lines(self, data: dict | None) -> list[str]:
        employments = data.get("employments") if isinstance(data, dict) else None
        if not isinstance(employments, list):
            return []
        lines = []
        for employment in employments:
            if isinstance(employment, dict):
                line = self._describe_employment(employment)
                if line:
                    lines.append(f"{EMPLOYMENT_LINE_INDENT}{line}")
        return lines

    def _describe_employment(self, employment: dict) -> str | None:
        position = self._position_label(employment.get("type"))
        organization = self._employment_organization_label(
            employment.get("organization")
        )
        heading = ", ".join(part for part in (position, organization) if part)
        if not heading:
            return None
        details = ", ".join(
            part
            for part in (
                _format_period(employment.get("startDate"), employment.get("endDate")),
                _format_percentage(employment.get("fullTimeEquivalentPercentage")),
            )
            if part
        )
        return f"{heading} ({details})" if details else heading

    def _position_label(self, type_uri: object) -> str | None:
        code = _uri_fragment(type_uri)
        if code is None:
            return None
        return self._position_labels().get(code, code)

    def _position_labels(self) -> dict[str, str]:
        if self._position_labels_cache is None:
            self._position_labels_cache = _parse_position_labels(
                self._get_path(CRISTIN_POSITION_PATH)
            )
        return self._position_labels_cache

    def _employment_organization_label(self, uri: object) -> str | None:
        if not isinstance(uri, str) or not uri.strip():
            return None
        return self.organization_label(uri) or _last_uri_segment(uri)

    def _get_path(self, path: str, authenticated: bool = False) -> dict | None:
        try:
            url = f"https://{self._api_client.api_domain}/{path}"
        except Exception as error:  # noqa: BLE001 - lookup must never block the update
            logger.debug("Could not resolve api domain for %s: %s", path, error)
            return None
        extra_headers = self._auth_headers() if authenticated else {}
        return self._get_url(url, extra_headers)

    def _auth_headers(self) -> dict[str, str]:
        try:
            return self._api_client.auth_header()
        except Exception as error:  # noqa: BLE001 - fall back to unauthenticated lookup
            logger.debug("Could not obtain auth token for entity lookup: %s", error)
            return {}

    def _get_url(
        self, url: str, extra_headers: dict[str, str] | None = None
    ) -> dict | None:
        try:
            response = requests.get(
                url,
                headers={"Accept": "application/json", **(extra_headers or {})},
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


def _display_label_person(data: dict | None) -> str | None:
    name = _display_name_person(data)
    national_id = _national_identification_number(data)
    if name and national_id:
        return f"{name}, fnr {national_id}"
    return name


def _national_identification_number(data: dict | None) -> str | None:
    if not isinstance(data, dict):
        return None
    identifiers = data.get("identifiers")
    if not isinstance(identifiers, list):
        return None
    for entry in identifiers:
        if (
            isinstance(entry, dict)
            and entry.get("type") == NATIONAL_IDENTIFICATION_NUMBER_TYPE
        ):
            value = entry.get("value")
            if isinstance(value, str) and value.strip():
                return value
    return None


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


def _parse_position_labels(data: dict | None) -> dict[str, str]:
    if not isinstance(data, dict) or not isinstance(data.get("positions"), list):
        return {}
    labels: dict[str, str] = {}
    for position in data["positions"]:
        if not isinstance(position, dict):
            continue
        code = _uri_fragment(position.get("id"))
        label = _from_label_map(position.get("labels"))
        if code and label:
            labels[code] = label
    return labels


def _uri_fragment(uri: object) -> str | None:
    if not isinstance(uri, str) or "#" not in uri:
        return None
    fragment = uri.rsplit("#", 1)[-1]
    return fragment if fragment.strip() else None


def _last_uri_segment(uri: str) -> str:
    return uri.rstrip("/").rsplit("/", 1)[-1]


def _format_period(start: object, end: object) -> str | None:
    start_date = _date_part(start)
    end_date = _date_part(end)
    if start_date is None and end_date is None:
        return None
    return f"{start_date or ''}–{end_date or ''}"


def _date_part(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.split("T", 1)[0]


def _format_percentage(value: object) -> str | None:
    if not isinstance(value, (int, float)):
        return None
    return f"{value:g}%"


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
