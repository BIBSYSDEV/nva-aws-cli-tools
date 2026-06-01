import json
import logging
import math
import mimetypes
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import cached_property
from pathlib import Path
from typing import Any, Protocol

import requests

logger = logging.getLogger(__name__)

# S3 multipart minimum part size (5 MiB) applies to every part except the last.
PART_SIZE = 5 * 1024 * 1024
DEFAULT_MIMETYPE = "application/octet-stream"
TOKEN_REFRESH_BUFFER_SECONDS = 30

FILE_TYPE_OPEN = "OpenFile"
FILE_TYPE_INTERNAL = "InternalFile"

PUBLISHER_VERSION_PUBLISHED = "PublishedVersion"
PUBLISHER_VERSION_ACCEPTED = "AcceptedVersion"

EXTERNAL_COMPLETE_TYPE = "ExternalCompleteUpload"

# HTTP header read by RequestUtil -> ThirdPartySystem.fromValue. Drives the
# importSource on the resource log entries (FileUploadedEvent/PublishedResourceEvent).
# Missing/unknown value falls back to Source.OTHER, so we must set it to "DLR".
SYSTEM_HEADER = "System"
SYSTEM_DLR = "DLR"

API_DOMAIN_PARAMETER = "/NVA/ApiDomain"


@dataclass
class ExternalClientToken:
    """Cognito client-credentials token for a single external client key.

    Built from an external-client key file (clientId / clientSecret / tokenUrl).
    Ownership of uploaded files is derived from which external client performs
    the upload, so each key maps to one institution/customer (customerUri).
    """

    client_id: str
    client_secret: str
    token_url: str
    customer_uri: str | None = None
    client_name: str | None = None
    _token: str | None = field(default=None, init=False, repr=False)
    _expires_at: datetime | None = field(default=None, init=False, repr=False)

    @classmethod
    def from_key_file(cls, path: str | Path) -> "ExternalClientToken":
        key = json.loads(Path(path).read_text())
        return cls(
            client_id=key["clientId"],
            client_secret=key["clientSecret"],
            token_url=key["tokenUrl"],
            customer_uri=key.get("customerUri"),
            client_name=key.get("clientName"),
        )

    def bearer(self) -> str:
        if self._token is None or self._is_expired():
            self._refresh()
        assert self._token is not None
        return self._token

    def _refresh(self) -> None:
        response = requests.post(
            self.token_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        )
        response.raise_for_status()
        body = response.json()
        self._token = body["access_token"]
        self._expires_at = datetime.now() + timedelta(seconds=body["expires_in"])

    def _is_expired(self) -> bool:
        if self._expires_at is None:
            return True
        return datetime.now() > self._expires_at - timedelta(
            seconds=TOKEN_REFRESH_BUFFER_SECONDS
        )


class FileSource(Protocol):
    @property
    def filename(self) -> str: ...

    @property
    def mimetype(self) -> str: ...

    @property
    def size(self) -> int: ...

    def read_part(self, part_number: int, part_size: int) -> bytes: ...


@dataclass
class LocalFileSource:
    path: str
    mimetype_override: str | None = None

    @property
    def filename(self) -> str:
        return os.path.basename(self.path)

    @property
    def mimetype(self) -> str:
        if self.mimetype_override:
            return self.mimetype_override
        guessed, _ = mimetypes.guess_type(self.path)
        return guessed or DEFAULT_MIMETYPE

    @property
    def size(self) -> int:
        return os.path.getsize(self.path)

    def read_part(self, part_number: int, part_size: int) -> bytes:
        with open(self.path, "rb") as file:
            file.seek((part_number - 1) * part_size)
            return file.read(part_size)


@dataclass
class S3ObjectSource:
    s3_client: Any
    bucket: str
    key: str
    filename_override: str | None = None
    mimetype_override: str | None = None

    @cached_property
    def _head(self) -> dict:
        return self.s3_client.head_object(Bucket=self.bucket, Key=self.key)

    @property
    def filename(self) -> str:
        if self.filename_override:
            return self.filename_override
        return os.path.basename(self.key)

    @property
    def mimetype(self) -> str:
        if self.mimetype_override:
            return self.mimetype_override
        content_type = self._head.get("ContentType")
        if content_type and content_type != "binary/octet-stream":
            return content_type
        guessed, _ = mimetypes.guess_type(self.key)
        return guessed or DEFAULT_MIMETYPE

    @property
    def size(self) -> int:
        return self._head["ContentLength"]

    def read_part(self, part_number: int, part_size: int) -> bytes:
        start = (part_number - 1) * part_size
        end = min(start + part_size, self.size) - 1
        response = self.s3_client.get_object(
            Bucket=self.bucket, Key=self.key, Range=f"bytes={start}-{end}"
        )
        return response["Body"].read()


@dataclass
class FileUploadApiService:
    api_domain: str
    token: ExternalClientToken
    system: str = SYSTEM_DLR

    def upload(
        self,
        publication_identifier: str,
        source: FileSource,
        *,
        file_type: str = FILE_TYPE_OPEN,
        license: str | None = None,
        publisher_version: str | None = None,
        embargo_date: str | None = None,
    ) -> dict:
        upload_id, key = self._create(publication_identifier, source)
        logger.info(
            "Started upload %s for %s (key %s, %d bytes)",
            upload_id,
            publication_identifier,
            key,
            source.size,
        )
        parts = self._upload_parts(publication_identifier, source, upload_id, key)
        return self._complete(
            publication_identifier,
            upload_id,
            key,
            parts,
            file_type,
            license,
            publisher_version,
            embargo_date,
        )

    def _create(
        self, publication_identifier: str, source: FileSource
    ) -> tuple[str, str]:
        body = self._post(
            publication_identifier,
            "create",
            {
                "filename": source.filename,
                "size": str(source.size),
                "mimetype": source.mimetype,
            },
        )
        return body["uploadId"], body["key"]

    def _upload_parts(
        self,
        publication_identifier: str,
        source: FileSource,
        upload_id: str,
        key: str,
    ) -> list[dict]:
        num_parts = max(1, math.ceil(source.size / PART_SIZE))
        parts = []
        for part_number in range(1, num_parts + 1):
            chunk = source.read_part(part_number, PART_SIZE)
            etag = self._upload_part(
                publication_identifier, upload_id, key, part_number, chunk
            )
            parts.append({"partNumber": part_number, "etag": etag})
            logger.info("  part %d/%d uploaded (etag %s)", part_number, num_parts, etag)
        return parts

    def _upload_part(
        self,
        publication_identifier: str,
        upload_id: str,
        key: str,
        part_number: int,
        chunk: bytes,
    ) -> str:
        prepare_body = self._post(
            publication_identifier,
            "prepare",
            {"uploadId": upload_id, "key": key, "number": str(part_number)},
        )
        presigned_url = prepare_body["url"]
        response = requests.put(presigned_url, data=chunk)
        response.raise_for_status()
        return response.headers["ETag"].strip('"')

    def _complete(
        self,
        publication_identifier: str,
        upload_id: str,
        key: str,
        parts: list[dict],
        file_type: str,
        license: str | None,
        publisher_version: str | None,
        embargo_date: str | None,
    ) -> dict:
        body = {
            "type": EXTERNAL_COMPLETE_TYPE,
            "uploadId": upload_id,
            "key": key,
            "parts": parts,
            "fileType": file_type,
        }
        if license is not None:
            body["license"] = license
        if publisher_version is not None:
            body["publisherVersion"] = publisher_version
        if embargo_date is not None:
            body["embargoDate"] = embargo_date
        return self._post(publication_identifier, "complete", body)

    def publish(self, publication_identifier: str) -> None:
        """Publish a draft directly (no ticket). Idempotent: 409/already-published is ok.

        Sends System: DLR so the PublishedResourceEvent log entry gets source DLR.
        """
        url = f"https://{self.api_domain}/publication/{publication_identifier}/publish"
        response = requests.post(url, headers=self._headers())
        if response.status_code == 409:
            logger.info("%s already published", publication_identifier)
            return
        response.raise_for_status()
        logger.info("Published %s (%d)", publication_identifier, response.status_code)

    def _post(self, publication_identifier: str, action: str, body: dict) -> dict:
        url = (
            f"https://{self.api_domain}/publication/"
            f"{publication_identifier}/file-upload/{action}"
        )
        response = requests.post(url, headers=self._headers(), json=body)
        response.raise_for_status()
        return response.json()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token.bearer()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            SYSTEM_HEADER: self.system,
        }


def resolve_api_domain(session: Any) -> str:
    response = session.client("ssm").get_parameter(Name=API_DOMAIN_PARAMETER)
    return response["Parameter"]["Value"]
