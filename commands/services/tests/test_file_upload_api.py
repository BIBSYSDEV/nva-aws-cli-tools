import json
from pathlib import Path

import pytest
import responses

from commands.services.file_upload_api import (
    EXTERNAL_COMPLETE_TYPE,
    FILE_TYPE_OPEN,
    PART_SIZE,
    SYSTEM_DLR,
    SYSTEM_HEADER,
    ExternalClientToken,
    FileUploadApiService,
    LocalFileSource,
)

API_DOMAIN = "api.example.org"
TOKEN_URL = "https://cognito.example.org/oauth2/token"
PUBLICATION_IDENTIFIER = "019de845-2706-7afd-bc2e-44d4-86d9-5192a042f61f"
CLIENT_ID = "client-id"
CLIENT_SECRET = "client-secret"
UPLOAD_ID = "upload-id-123"
SERVER_KEY = "server-generated-key-uuid"
PRESIGNED_URL = "https://s3.example.org/presigned"
ETAG = "abc123etag"


def _key_file(tmp_path: Path) -> Path:
    path = tmp_path / "key.json"
    path.write_text(
        json.dumps(
            {
                "clientId": CLIENT_ID,
                "clientSecret": CLIENT_SECRET,
                "tokenUrl": TOKEN_URL,
                "clientName": "dlr-integration",
                "customerUri": "https://api.example.org/customer/abc",
            }
        )
    )
    return path


def _add_token_response(expires_in: int = 3600) -> None:
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={"access_token": "the-access-token", "expires_in": expires_in},
    )


def _add_create_response() -> None:
    responses.add(
        responses.POST,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}/file-upload/create",
        json={"uploadId": UPLOAD_ID, "key": SERVER_KEY},
    )


def _add_prepare_response() -> None:
    responses.add(
        responses.POST,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}/file-upload/prepare",
        json={"url": PRESIGNED_URL},
    )


def _add_put_response() -> None:
    responses.add(responses.PUT, PRESIGNED_URL, headers={"ETag": f'"{ETAG}"'})


def _add_complete_response(returned_file: dict | None = None) -> None:
    body = returned_file or {
        "identifier": "file-identifier",
        "name": "file.bin",
        "mimeType": "application/octet-stream",
        "size": 10,
    }
    responses.add(
        responses.POST,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}/file-upload/complete",
        json=body,
    )


def _build_service(tmp_path: Path) -> FileUploadApiService:
    token = ExternalClientToken.from_key_file(_key_file(tmp_path))
    return FileUploadApiService(api_domain=API_DOMAIN, token=token)


@responses.activate
def test_token_refresh_sends_client_credentials_grant(tmp_path: Path) -> None:
    _add_token_response()
    token = ExternalClientToken.from_key_file(_key_file(tmp_path))

    bearer = token.bearer()

    assert bearer == "the-access-token"
    grant_calls = [c for c in responses.calls if c.request.url == TOKEN_URL]
    assert len(grant_calls) == 1
    body = grant_calls[0].request.body
    assert "grant_type=client_credentials" in body
    assert f"client_id={CLIENT_ID}" in body


@responses.activate
def test_token_is_cached_across_calls(tmp_path: Path) -> None:
    _add_token_response()
    token = ExternalClientToken.from_key_file(_key_file(tmp_path))

    token.bearer()
    token.bearer()

    grant_calls = [c for c in responses.calls if c.request.url == TOKEN_URL]
    assert len(grant_calls) == 1


@responses.activate
def test_upload_runs_full_multipart_flow_with_dlr_header(tmp_path: Path) -> None:
    _add_token_response()
    _add_create_response()
    _add_prepare_response()
    _add_put_response()
    _add_complete_response()

    file_path = tmp_path / "small.bin"
    file_path.write_bytes(b"hello world")
    service = _build_service(tmp_path)

    result = service.upload(
        PUBLICATION_IDENTIFIER,
        LocalFileSource(str(file_path)),
        license="https://creativecommons.org/licenses/by/4.0/",
    )

    assert result["identifier"] == "file-identifier"
    _assert_create_payload(file_path)
    _assert_prepare_payload()
    _assert_complete_payload()
    _assert_system_dlr_on_all_api_calls()


def _assert_create_payload(file_path: Path) -> None:
    create_call = _find_call("file-upload/create")
    body = json.loads(create_call.request.body)
    assert body == {
        "filename": file_path.name,
        "size": str(file_path.stat().st_size),
        "mimetype": "application/octet-stream",
    }


def _assert_prepare_payload() -> None:
    prepare_call = _find_call("file-upload/prepare")
    body = json.loads(prepare_call.request.body)
    assert body == {"uploadId": UPLOAD_ID, "key": SERVER_KEY, "number": "1"}


def _assert_complete_payload() -> None:
    complete_call = _find_call("file-upload/complete")
    body = json.loads(complete_call.request.body)
    assert body["type"] == EXTERNAL_COMPLETE_TYPE
    assert body["uploadId"] == UPLOAD_ID
    assert body["key"] == SERVER_KEY
    assert body["fileType"] == FILE_TYPE_OPEN
    assert body["license"] == "https://creativecommons.org/licenses/by/4.0/"
    assert body["parts"] == [{"partNumber": 1, "etag": ETAG}]
    assert "publisherVersion" not in body
    assert "embargoDate" not in body


def _assert_system_dlr_on_all_api_calls() -> None:
    api_calls = [
        call
        for call in responses.calls
        if call.request.url.startswith(f"https://{API_DOMAIN}/")
    ]
    for call in api_calls:
        assert call.request.headers.get(SYSTEM_HEADER) == SYSTEM_DLR, (
            f"Missing System:DLR on {call.request.url}"
        )


def _find_call(needle: str):
    matches = [call for call in responses.calls if needle in call.request.url]
    assert len(matches) == 1, f"Expected one call to {needle}, got {len(matches)}"
    return matches[0]


@responses.activate
def test_upload_splits_into_multiple_parts(tmp_path: Path) -> None:
    _add_token_response()
    _add_create_response()
    responses.add(
        responses.POST,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}/file-upload/prepare",
        json={"url": PRESIGNED_URL},
    )
    _add_put_response()
    _add_complete_response()

    file_path = tmp_path / "big.bin"
    file_path.write_bytes(b"x" * (PART_SIZE + 100))
    service = _build_service(tmp_path)

    service.upload(PUBLICATION_IDENTIFIER, LocalFileSource(str(file_path)))

    prepare_calls = [
        c for c in responses.calls if "file-upload/prepare" in c.request.url
    ]
    assert len(prepare_calls) == 2
    part_numbers = [json.loads(c.request.body)["number"] for c in prepare_calls]
    assert part_numbers == ["1", "2"]

    complete_call = _find_call("file-upload/complete")
    parts = json.loads(complete_call.request.body)["parts"]
    assert [part["partNumber"] for part in parts] == [1, 2]
    assert all(part["etag"] == ETAG for part in parts)


@responses.activate
def test_upload_passes_publisher_version_and_embargo(tmp_path: Path) -> None:
    _add_token_response()
    _add_create_response()
    _add_prepare_response()
    _add_put_response()
    _add_complete_response()

    file_path = tmp_path / "small.bin"
    file_path.write_bytes(b"hi")
    service = _build_service(tmp_path)

    service.upload(
        PUBLICATION_IDENTIFIER,
        LocalFileSource(str(file_path)),
        publisher_version="AcceptedVersion",
        embargo_date="2026-12-31",
    )

    complete_call = _find_call("file-upload/complete")
    body = json.loads(complete_call.request.body)
    assert body["publisherVersion"] == "AcceptedVersion"
    assert body["embargoDate"] == "2026-12-31"


@responses.activate
def test_create_publication_posts_minimal_body_with_dlr_header(
    tmp_path: Path,
) -> None:
    _add_token_response()
    responses.add(
        responses.POST,
        f"https://{API_DOMAIN}/publication",
        json={"identifier": "new-pub-id", "status": "DRAFT"},
        status=201,
    )
    service = _build_service(tmp_path)

    result = service.create_publication("My test title")

    assert result["identifier"] == "new-pub-id"
    create_call = _find_call(f"https://{API_DOMAIN}/publication")
    body = json.loads(create_call.request.body)
    assert body["type"] == "Publication"
    assert body["entityDescription"]["mainTitle"] == "My test title"
    assert (
        body["entityDescription"]["reference"]["publicationInstance"]["type"]
        == "OtherPresentation"
    )
    assert (
        body["entityDescription"]["reference"]["publicationContext"]["type"] == "Event"
    )
    contributors = body["entityDescription"]["contributors"]
    assert len(contributors) == 1
    assert contributors[0]["role"]["type"] == "Creator"
    publication_date = body["entityDescription"]["publicationDate"]
    assert publication_date["type"] == "PublicationDate"
    assert publication_date["year"].isdigit()
    event_time = body["entityDescription"]["reference"]["publicationContext"]["time"]
    assert event_time["type"] == "Period"
    assert "T00:00:00" in event_time["from"]
    assert create_call.request.headers.get(SYSTEM_HEADER) == SYSTEM_DLR


EXISTING_FILE_ARTIFACT = {
    "type": "OpenFile",
    "identifier": "11111111-1111-1111-1111-111111111111",
    "name": "existing-upload.pdf",
}


@responses.activate
def test_add_associated_links_skips_dupes_and_puts_new_ones(
    tmp_path: Path,
) -> None:
    _add_token_response()
    existing_link = {
        "type": "AssociatedLink",
        "id": "https://existing.example/already",
        "relation": "sameAs",
    }
    existing = {
        "type": "Publication",
        "associatedArtifacts": [EXISTING_FILE_ARTIFACT, existing_link],
    }
    responses.add(
        responses.GET,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}",
        json=existing,
    )
    responses.add(
        responses.PUT,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}",
        json={
            "identifier": PUBLICATION_IDENTIFIER,
            "associatedArtifacts": existing["associatedArtifacts"]
            + [
                {
                    "type": "AssociatedLink",
                    "id": "https://new.example/panopto",
                    "relation": "sameAs",
                }
            ],
        },
    )
    service = _build_service(tmp_path)

    added, skipped = service.add_associated_links(
        PUBLICATION_IDENTIFIER,
        [
            "https://existing.example/already",
            "https://new.example/panopto",
        ],
    )

    assert added == 1
    assert skipped == 1
    put_call = next(c for c in responses.calls if c.request.method == "PUT")
    body = json.loads(put_call.request.body)
    assert body["type"] == "Publication"
    assert body["identifier"] == PUBLICATION_IDENTIFIER
    artifacts = body["associatedArtifacts"]
    assert EXISTING_FILE_ARTIFACT in artifacts
    links = [a for a in artifacts if a.get("type") == "AssociatedLink"]
    assert len(links) == 2
    new_link = next(a for a in links if a["id"] == "https://new.example/panopto")
    assert new_link["relation"] == "sameAs"
    assert put_call.request.headers.get(SYSTEM_HEADER) == SYSTEM_DLR


@responses.activate
def test_add_associated_links_round_trips_files_and_metadata(
    tmp_path: Path,
) -> None:
    _add_token_response()
    entity_description = {"type": "EntityDescription", "mainTitle": "Title"}
    project = {"type": "ResearchProject", "id": "https://example.org/p/1"}
    funding = {"type": "ConfirmedFunding", "id": "https://example.org/f/1"}
    responses.add(
        responses.GET,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}",
        json={
            "type": "Publication",
            "entityDescription": entity_description,
            "projects": [project],
            "fundings": [funding],
            "rightsHolder": "Some Holder",
            "associatedArtifacts": [EXISTING_FILE_ARTIFACT],
        },
    )
    responses.add(
        responses.PUT,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}",
        json={
            "identifier": PUBLICATION_IDENTIFIER,
            "associatedArtifacts": [
                EXISTING_FILE_ARTIFACT,
                {
                    "type": "AssociatedLink",
                    "id": "https://new.example/x",
                    "relation": "sameAs",
                },
            ],
        },
    )
    service = _build_service(tmp_path)

    service.add_associated_links(PUBLICATION_IDENTIFIER, ["https://new.example/x"])

    put_call = next(c for c in responses.calls if c.request.method == "PUT")
    body = json.loads(put_call.request.body)
    assert body["entityDescription"] == entity_description
    assert body["projects"] == [project]
    assert body["fundings"] == [funding]
    assert body["rightsHolder"] == "Some Holder"
    assert EXISTING_FILE_ARTIFACT in body["associatedArtifacts"]


@responses.activate
def test_add_associated_links_raises_when_response_omits_new_value(
    tmp_path: Path,
) -> None:
    _add_token_response()
    responses.add(
        responses.GET,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}",
        json={"type": "Publication", "associatedArtifacts": []},
    )
    responses.add(
        responses.PUT,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}",
        json={"identifier": PUBLICATION_IDENTIFIER, "associatedArtifacts": []},
    )
    service = _build_service(tmp_path)

    with pytest.raises(RuntimeError, match="silently dropped"):
        service.add_associated_links(
            PUBLICATION_IDENTIFIER, ["https://lost.example/url"]
        )


@responses.activate
def test_add_associated_links_no_put_when_all_dupes(tmp_path: Path) -> None:
    _add_token_response()
    responses.add(
        responses.GET,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}",
        json={
            "type": "Publication",
            "associatedArtifacts": [
                {
                    "type": "AssociatedLink",
                    "id": "https://known.example/x",
                    "relation": "sameAs",
                }
            ],
        },
    )
    service = _build_service(tmp_path)

    added, skipped = service.add_associated_links(
        PUBLICATION_IDENTIFIER,
        ["https://known.example/x"],
    )

    assert added == 0
    assert skipped == 1
    put_calls = [c for c in responses.calls if c.request.method == "PUT"]
    assert put_calls == []


@responses.activate
def test_existing_file_signatures_returns_name_size_pairs(tmp_path: Path) -> None:
    _add_token_response()
    responses.add(
        responses.GET,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}",
        json={
            "type": "Publication",
            "associatedArtifacts": [
                {"type": "OpenFile", "name": "a.pdf", "size": 100},
                {"type": "PendingOpenFile", "name": "b.mp4", "size": 200},
                {"type": "AssociatedLink", "id": "https://x", "relation": "sameAs"},
            ],
        },
    )
    service = _build_service(tmp_path)

    signatures = service.existing_file_signatures(PUBLICATION_IDENTIFIER)

    assert signatures == {("a.pdf", 100), ("b.mp4", 200)}


@responses.activate
def test_publish_sends_system_dlr_and_empty_body(tmp_path: Path) -> None:
    _add_token_response()
    responses.add(
        responses.POST,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}/publish",
        status=202,
    )
    service = _build_service(tmp_path)

    service.publish(PUBLICATION_IDENTIFIER)

    publish_call = _find_call("/publish")
    assert publish_call.request.headers.get(SYSTEM_HEADER) == SYSTEM_DLR
    assert not publish_call.request.body


@responses.activate
def test_publish_treats_409_as_already_published(tmp_path: Path) -> None:
    _add_token_response()
    responses.add(
        responses.POST,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}/publish",
        status=409,
        json={"message": "already published"},
    )
    service = _build_service(tmp_path)

    service.publish(PUBLICATION_IDENTIFIER)


@responses.activate
def test_publish_raises_on_403(tmp_path: Path) -> None:
    _add_token_response()
    responses.add(
        responses.POST,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}/publish",
        status=403,
        json={"message": "forbidden"},
    )
    service = _build_service(tmp_path)

    with pytest.raises(Exception):  # noqa: B017
        service.publish(PUBLICATION_IDENTIFIER)


@responses.activate
def test_delete_publication_sends_delete_with_dlr_header(tmp_path: Path) -> None:
    _add_token_response()
    responses.add(
        responses.DELETE,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}",
        status=202,
    )
    service = _build_service(tmp_path)

    service.delete_publication(PUBLICATION_IDENTIFIER)

    delete_call = next(c for c in responses.calls if c.request.method == "DELETE")
    assert delete_call.request.headers.get(SYSTEM_HEADER) == SYSTEM_DLR


@responses.activate
def test_delete_publication_raises_when_not_draft(tmp_path: Path) -> None:
    _add_token_response()
    responses.add(
        responses.DELETE,
        f"https://{API_DOMAIN}/publication/{PUBLICATION_IDENTIFIER}",
        status=400,
        json={"message": "Only unpublished publication can be deleted"},
    )
    service = _build_service(tmp_path)

    with pytest.raises(Exception):  # noqa: B017
        service.delete_publication(PUBLICATION_IDENTIFIER)


def test_local_file_source_reads_parts(tmp_path: Path) -> None:
    file_path = tmp_path / "data.bin"
    payload = b"ABCDEFGHIJ"
    file_path.write_bytes(payload)
    source = LocalFileSource(str(file_path))

    assert source.size == len(payload)
    assert source.read_part(1, 4) == b"ABCD"
    assert source.read_part(2, 4) == b"EFGH"
    assert source.read_part(3, 4) == b"IJ"


def test_local_file_source_mimetype_override(tmp_path: Path) -> None:
    file_path = tmp_path / "unknown.dat"
    file_path.write_bytes(b"x")
    source = LocalFileSource(str(file_path), mimetype_override="video/mp4")
    assert source.mimetype == "video/mp4"
