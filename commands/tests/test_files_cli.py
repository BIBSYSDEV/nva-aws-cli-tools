import json
import zlib
from pathlib import Path

import boto3
import pytest
import responses
from boto3.dynamodb.types import Binary
from click.testing import CliRunner
from moto import mock_aws

from cli import cli

DEFAULT_OWNER = "ntnu-dlr-import-integration@ntnu"
NON_DLR_OWNER = "someone@uib.no"

API_DOMAIN = "api.example.org"
TOKEN_URL = "https://cognito.example.org/oauth2/token"
RESULT_ID_NTNU = "019de845-2706-7afd-bc2e-ntnu000000000001"
RESULT_ID_VID = "019de845-2706-7afd-bc2e-vid0000000000001"
RESULT_ID_USN = "019de845-2706-7afd-bc2e-usn0000000000001"


def _key_file(tmp_path: Path) -> Path:
    path = tmp_path / "key.json"
    path.write_text(
        json.dumps(
            {
                "clientId": "client-id",
                "clientSecret": "client-secret",
                "tokenUrl": TOKEN_URL,
                "clientName": "dlr",
                "customerUri": "https://api.example.org/customer/x",
            }
        )
    )
    return path


def _manifest_file(tmp_path: Path) -> Path:
    manifest = {
        "dlr-1": {
            "result_id": RESULT_ID_NTNU,
            "license": "https://creativecommons.org/licenses/by/4.0/",
            "handle": "https://hdl.handle.net/11250/3107928",
            "content": [
                {
                    "dlr_content": "main.mp4",
                    "dlr_content_identifier": "s3-key-ntnu-main",
                    "dlr_content_mime_type": "video/mp4",
                    "dlr_content_size_bytes": "100",
                    "dlr_content_type": "file",
                    "dlr_submitter_email": "user@ntnu.no",
                },
                {
                    "dlr_content": "thumb.jpg",
                    "dlr_content_identifier": "s3-key-ntnu-thumb",
                    "dlr_content_mime_type": "image/jpeg",
                    "dlr_content_size_bytes": "10",
                    "dlr_content_type": "file",
                    "dlr_content_generated": "true",
                    "dlr_submitter_email": "user@ntnu.no",
                },
                {
                    "dlr_content": "https://example.org",
                    "dlr_content_identifier": "ignored-link",
                    "dlr_content_type": "link",
                    "dlr_submitter_email": "user@ntnu.no",
                },
            ],
        },
        "dlr-2": {
            "result_id": RESULT_ID_VID,
            "handle": "https://hdl.handle.net/11250/9999999",
            "content": [
                {
                    "dlr_content": "vid.pdf",
                    "dlr_content_identifier": "s3-key-vid-main",
                    "dlr_content_mime_type": "application/pdf",
                    "dlr_content_type": "file",
                    "dlr_submitter_email": "user@vid.no",
                }
            ],
        },
        "dlr-3-no-handle": {
            "result_id": "no-handle-id",
            "content": [
                {
                    "dlr_content": "x.pdf",
                    "dlr_content_identifier": "s3-key-x",
                    "dlr_content_type": "file",
                    "dlr_submitter_email": "user@vid.no",
                }
            ],
        },
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return path


def _seed_ssm() -> None:
    boto3.client("ssm", region_name="eu-west-1").put_parameter(
        Name="/NVA/ApiDomain", Value=API_DOMAIN, Type="String"
    )


@mock_aws
def test_upload_manifest_dry_run_filters_by_domain_and_skips_non_files(
    tmp_path: Path,
) -> None:
    manifest = _manifest_file(tmp_path)
    key = _key_file(tmp_path)

    result = CliRunner().invoke(
        cli,
        [
            "--quiet",
            "files",
            "upload-manifest",
            str(manifest),
            "--key-file",
            str(key),
            "--institution",
            "ntnu.no,hist.no",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Resources: 1" in result.output
    assert "Files to upload: 1" in result.output
    assert "s3-key-ntnu-main" in result.output
    assert "s3-key-ntnu-thumb" not in result.output
    assert "ignored-link" not in result.output
    assert "s3-key-vid-main" not in result.output


@mock_aws
@responses.activate
def test_publish_one_posts_to_publish_endpoint_with_dlr_header(
    tmp_path: Path,
) -> None:
    _seed_ssm()
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={"access_token": "token", "expires_in": 3600},
    )
    responses.add(
        responses.POST,
        f"https://{API_DOMAIN}/publication/{RESULT_ID_NTNU}/publish",
        status=202,
    )
    key = _key_file(tmp_path)

    result = CliRunner().invoke(
        cli,
        [
            "--quiet",
            "files",
            "publish-one",
            "--key-file",
            str(key),
            "--publication",
            RESULT_ID_NTNU,
        ],
    )

    assert result.exit_code == 0, result.output
    publish_calls = [call for call in responses.calls if "/publish" in call.request.url]
    assert len(publish_calls) == 1
    assert publish_calls[0].request.headers.get("System") == "DLR"


def test_extract_handles_strips_prefix_and_filters_by_institution(
    tmp_path: Path,
) -> None:
    manifest = _manifest_file(tmp_path)
    output = tmp_path / "handles.txt"

    result = CliRunner().invoke(
        cli,
        [
            "--quiet",
            "files",
            "extract-handles",
            str(manifest),
            "--institution",
            "vid.no",
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 0, result.output
    written = output.read_text().splitlines()
    assert written == ["11250/9999999"]


def test_extract_handles_all_institutions_to_stdout(tmp_path: Path) -> None:
    manifest = _manifest_file(tmp_path)

    result = CliRunner().invoke(
        cli, ["--quiet", "files", "extract-handles", str(manifest)]
    )

    assert result.exit_code == 0, result.output
    lines = result.output.strip().splitlines()
    assert set(lines) == {"11250/3107928", "11250/9999999"}


@mock_aws
def test_check_source_tallies_per_resource_and_grand_total(
    tmp_path: Path,
) -> None:
    _seed_resources_table(
        [
            _log_entry_row(RESULT_ID_NTNU, "log-1", "DLR"),
            _log_entry_row(RESULT_ID_NTNU, "log-2", "OTHER"),
            _log_entry_row(RESULT_ID_NTNU, "log-3", "CRISTIN"),
            _log_entry_row(RESULT_ID_VID, "log-4", "DLR"),
        ]
    )
    manifest = _manifest_file(tmp_path)

    result = CliRunner().invoke(
        cli, ["--quiet", "files", "check-source", str(manifest)]
    )

    assert result.exit_code == 0, result.output
    assert f"{RESULT_ID_NTNU}  logs=3" in result.output
    assert "CRISTIN=1" in result.output
    assert "DLR=1" in result.output
    assert "OTHER=1" in result.output
    assert f"{RESULT_ID_VID}  logs=1" in result.output
    assert "Resources with any OTHER: 1" in result.output
    assert "LogEntries total: 4" in result.output


@mock_aws
def test_check_source_filters_by_institution_and_flags_missing(
    tmp_path: Path,
) -> None:
    _seed_resources_table(
        [_log_entry_row(RESULT_ID_NTNU, "log-1", "DLR")],
    )
    manifest = _manifest_file(tmp_path)

    result = CliRunner().invoke(
        cli,
        [
            "--quiet",
            "files",
            "check-source",
            str(manifest),
            "--institution",
            "vid.no",
        ],
    )

    assert result.exit_code == 0, result.output
    assert RESULT_ID_NTNU not in result.output
    assert f"{RESULT_ID_VID}  MISSING" in result.output


@mock_aws
def test_fix_log_source_dry_run_lists_candidates_without_writing(
    tmp_path: Path,
) -> None:
    table = _seed_resources_table(
        [
            _resource_row(RESULT_ID_NTNU),
            _log_entry_row(RESULT_ID_NTNU, "log-1", "OTHER"),
            _log_entry_row(RESULT_ID_NTNU, "log-2", "DLR"),
        ]
    )
    manifest = _manifest_file(tmp_path)

    result = CliRunner().invoke(
        cli, ["--quiet", "files", "fix-log-source", str(manifest)]
    )

    assert result.exit_code == 0, result.output
    assert "DRY-RUN" in result.output
    assert "log=log-1" in result.output
    assert "log=log-2" not in result.output
    assert "candidates=1 updated=0" in result.output
    # Verify nothing was written
    log_entry_rows = [
        row for row in table.scan()["Items"] if str(row["SK0"]).startswith("LogEntry:")
    ]
    other_rows = [
        row
        for row in log_entry_rows
        if row["data"]["importSource"]["source"] == "OTHER"
    ]
    assert len(other_rows) == 1


@mock_aws
def test_fix_log_source_apply_updates_rows_and_continues_on_failure(
    tmp_path: Path,
) -> None:
    table = _seed_resources_table(
        [
            _resource_row(RESULT_ID_NTNU),
            _resource_row(RESULT_ID_VID),
            _log_entry_row(RESULT_ID_NTNU, "log-1", "OTHER"),
            _log_entry_row(RESULT_ID_VID, "log-2", "OTHER"),
        ]
    )
    manifest = _manifest_file(tmp_path)

    result = CliRunner().invoke(
        cli,
        ["--quiet", "files", "fix-log-source", str(manifest), "--no-dry-run"],
    )

    assert result.exit_code == 0, result.output
    assert "candidates=2 updated=2 failed=0" in result.output
    sources = {
        row["SK0"]: row["data"]["importSource"]["source"]
        for row in table.scan()["Items"]
        if str(row["SK0"]).startswith("LogEntry:")
    }
    assert sources["LogEntry:log-1"] == "DLR"
    assert sources["LogEntry:log-2"] == "DLR"


@mock_aws
def test_fix_log_source_skips_partition_with_mismatched_owner(
    tmp_path: Path,
) -> None:
    table = _seed_resources_table(
        [
            _resource_row(RESULT_ID_NTNU, owner=DEFAULT_OWNER),
            _resource_row(RESULT_ID_VID, owner=NON_DLR_OWNER),
            _log_entry_row(RESULT_ID_NTNU, "log-1", "OTHER"),
            _log_entry_row(RESULT_ID_VID, "log-2", "OTHER"),
        ]
    )
    manifest = _manifest_file(tmp_path)

    result = CliRunner().invoke(
        cli,
        ["--quiet", "files", "fix-log-source", str(manifest), "--no-dry-run"],
    )

    assert result.exit_code == 0, result.output
    assert f"SKIP {RESULT_ID_VID}" in result.output
    assert "OWNER-MISMATCH" in result.output
    assert "candidates=1" in result.output
    assert "updated=1" in result.output
    assert "failed=0" in result.output
    sources = {
        row["SK0"]: row["data"]["importSource"]["source"]
        for row in table.scan()["Items"]
        if str(row["SK0"]).startswith("LogEntry:")
    }
    assert sources["LogEntry:log-1"] == "DLR"
    assert sources["LogEntry:log-2"] == "OTHER"


@mock_aws
def test_fix_log_source_force_bypasses_owner_check(
    tmp_path: Path,
) -> None:
    table = _seed_resources_table(
        [
            _resource_row(RESULT_ID_VID, owner=NON_DLR_OWNER),
            _log_entry_row(RESULT_ID_VID, "log-2", "OTHER"),
        ]
    )
    manifest = _manifest_file(tmp_path)

    result = CliRunner().invoke(
        cli,
        [
            "--quiet",
            "files",
            "fix-log-source",
            str(manifest),
            "--no-dry-run",
            "--force",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "owner_gate=<disabled>" in result.output
    assert "candidates=1 updated=1" in result.output
    sources = {
        row["SK0"]: row["data"]["importSource"]["source"]
        for row in table.scan()["Items"]
        if str(row["SK0"]).startswith("LogEntry:")
    }
    assert sources["LogEntry:log-2"] == "DLR"


@mock_aws
def test_check_source_marks_owner_mismatch_without_writing(
    tmp_path: Path,
) -> None:
    _seed_resources_table(
        [
            _resource_row(RESULT_ID_NTNU, owner=DEFAULT_OWNER),
            _resource_row(RESULT_ID_VID, owner=NON_DLR_OWNER),
            _log_entry_row(RESULT_ID_NTNU, "log-1", "DLR"),
            _log_entry_row(RESULT_ID_VID, "log-2", "OTHER"),
        ]
    )
    manifest = _manifest_file(tmp_path)

    result = CliRunner().invoke(
        cli, ["--quiet", "files", "check-source", str(manifest)]
    )

    assert result.exit_code == 0, result.output
    assert f"{RESULT_ID_NTNU}  logs=1" in result.output
    assert f"owner={DEFAULT_OWNER}" in result.output
    assert "OWNER-MISMATCH" in result.output
    assert "Resources with owner mismatch:" in result.output


def _log_entry_row(result_id: str, log_id: str, source: str) -> dict:
    return {
        "PK0": f"Resource:{result_id}",
        "SK0": f"LogEntry:{log_id}",
        "data": {
            "topic": "FileUploadedEvent",
            "importSource": {"source": source, "archive": None},
        },
    }


def _resource_row(result_id: str, owner: str = DEFAULT_OWNER) -> dict:
    payload = json.dumps(
        {
            "resourceOwner": {
                "owner": owner,
                "ownerAffiliation": "https://example.org/org/1",
            },
            "status": "DRAFT",
        }
    ).encode("utf-8")
    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    compressed = compressor.compress(payload) + compressor.flush()
    return {
        "PK0": f"Resource:{result_id}",
        "SK0": f"Resource:{result_id}",
        "data": Binary(compressed),
    }


@pytest.fixture(autouse=True)
def _set_aws_region(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")


def _seed_resources_table(rows: list[dict]) -> object:
    dynamodb = boto3.resource("dynamodb", region_name="eu-west-1")
    table = dynamodb.create_table(
        TableName="nva-publication-resources",
        KeySchema=[
            {"AttributeName": "PK0", "KeyType": "HASH"},
            {"AttributeName": "SK0", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK0", "AttributeType": "S"},
            {"AttributeName": "SK0", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    for row in rows:
        table.put_item(Item=row)
    return table
