import gzip
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from commands.services.s3_versions import (
    build_git_history,
    decompress_if_needed,
    download_versions,
    fetch_versions,
    sanitize_to_folder_name,
    try_pretty_json,
)


def test_sanitize_to_folder_name_replaces_slashes():
    assert sanitize_to_folder_name("bucket/some/key") == "bucket_some_key"


def test_sanitize_to_folder_name_replaces_dots():
    assert sanitize_to_folder_name("file.name.gz") == "file_name_gz"


def test_sanitize_to_folder_name_keeps_alphanumeric_and_dash():
    assert sanitize_to_folder_name("my-key_123") == "my-key_123"


def test_sanitize_to_folder_name_strips_leading_trailing_underscores():
    assert sanitize_to_folder_name("/leading/trailing/") == "leading_trailing"


def test_decompress_if_needed_decompresses_gz():
    original = b"hello world"
    compressed = gzip.compress(original)
    result = decompress_if_needed(compressed, "file.gz")
    assert result == original


def test_decompress_if_needed_returns_data_unchanged_for_non_gz():
    data = b"plain data"
    result = decompress_if_needed(data, "file.json")
    assert result == data


def test_decompress_if_needed_returns_raw_on_corrupt_gz(caplog):
    corrupt = b"not gzip data"
    result = decompress_if_needed(corrupt, "file.gz")
    assert result == corrupt
    assert "Failed to decompress gz data" in caplog.text


def test_try_pretty_json_formats_valid_json():
    raw = b'{"b":2,"a":1}'
    result = try_pretty_json(raw)
    parsed = json.loads(result)
    assert parsed == {"a": 1, "b": 2}
    assert b"\n" in result


def test_try_pretty_json_returns_original_on_invalid_json():
    raw = b"not json"
    result = try_pretty_json(raw)
    assert result == raw


def make_version(
    version_id: str, last_modified: datetime, key: str = "resources/obj.gz"
) -> dict:
    return {
        "Key": key,
        "VersionId": version_id,
        "LastModified": last_modified,
    }


def test_fetch_versions_returns_sorted_versions():
    s3_client = MagicMock()
    paginator = MagicMock()
    s3_client.get_paginator.return_value = paginator

    v1 = make_version("v1", datetime(2024, 1, 1, tzinfo=timezone.utc))
    v2 = make_version("v2", datetime(2024, 1, 3, tzinfo=timezone.utc))
    v3 = make_version("v3", datetime(2024, 1, 2, tzinfo=timezone.utc))

    paginator.paginate.return_value = [{"Versions": [v2, v3, v1]}]

    versions = fetch_versions(s3_client, "my-bucket", "resources/obj.gz")

    assert [v["VersionId"] for v in versions] == ["v1", "v3", "v2"]


def test_fetch_versions_filters_by_exact_key():
    s3_client = MagicMock()
    paginator = MagicMock()
    s3_client.get_paginator.return_value = paginator

    target = make_version(
        "v1", datetime(2024, 1, 1, tzinfo=timezone.utc), key="resources/obj.gz"
    )
    other = make_version(
        "v2", datetime(2024, 1, 2, tzinfo=timezone.utc), key="resources/obj-other.gz"
    )

    paginator.paginate.return_value = [{"Versions": [target, other]}]

    versions = fetch_versions(s3_client, "my-bucket", "resources/obj.gz")

    assert len(versions) == 1
    assert versions[0]["VersionId"] == "v1"


def test_download_versions_creates_files(tmp_path: Path):
    s3_client = MagicMock()
    paginator = MagicMock()
    s3_client.get_paginator.return_value = paginator

    content = json.dumps({"id": "abc"}).encode()
    version = make_version(
        "abc123",
        datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        key="resources/obj.json",
    )
    paginator.paginate.return_value = [{"Versions": [version]}]
    s3_client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=content))
    }

    output_dir = download_versions(
        s3_client, "my-bucket", "resources/obj.json", str(tmp_path)
    )

    files = list(output_dir.iterdir())
    assert len(files) == 1
    assert json.loads(files[0].read_bytes()) == {"id": "abc"}


def test_download_versions_decompresses_gz(tmp_path: Path):
    s3_client = MagicMock()
    paginator = MagicMock()
    s3_client.get_paginator.return_value = paginator

    original = {"id": "compressed"}
    compressed = gzip.compress(json.dumps(original).encode())
    version = make_version("v1", datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc))
    paginator.paginate.return_value = [{"Versions": [version]}]
    s3_client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=compressed))
    }

    output_dir = download_versions(
        s3_client, "my-bucket", "resources/obj.gz", str(tmp_path)
    )

    files = list(output_dir.iterdir())
    assert len(files) == 1
    assert json.loads(files[0].read_bytes()) == original


def test_download_versions_raises_when_no_versions(tmp_path: Path):
    s3_client = MagicMock()
    paginator = MagicMock()
    s3_client.get_paginator.return_value = paginator
    paginator.paginate.return_value = [{"Versions": []}]

    with pytest.raises(ValueError, match="No versions found"):
        download_versions(s3_client, "my-bucket", "resources/obj.gz", str(tmp_path))


def test_download_versions_skips_existing_files(tmp_path: Path):
    s3_client = MagicMock()
    paginator = MagicMock()
    s3_client.get_paginator.return_value = paginator

    version = make_version("v1", datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc))
    paginator.paginate.return_value = [{"Versions": [version]}]

    folder_name = "resources_obj_gz"
    output_dir = tmp_path / folder_name
    output_dir.mkdir()
    existing_file = output_dir / "20240601_120000_v1"
    existing_file.write_bytes(b"existing")

    download_versions(s3_client, "my-bucket", "resources/obj.gz", str(tmp_path))

    s3_client.get_object.assert_not_called()
    assert existing_file.read_bytes() == b"existing"


def test_build_git_history_creates_commits(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("GIT_AUTHOR_NAME", "Test")
    monkeypatch.setenv("GIT_AUTHOR_EMAIL", "test@test.com")
    monkeypatch.setenv("GIT_COMMITTER_NAME", "Test")
    monkeypatch.setenv("GIT_COMMITTER_EMAIL", "test@test.com")
    version_dir = tmp_path / "versions"
    version_dir.mkdir()

    (version_dir / "20240101_120000_v1").write_text(json.dumps({"version": 1}))
    (version_dir / "20240102_120000_v2").write_text(json.dumps({"version": 2}))

    build_git_history(version_dir)

    result = subprocess.run(
        ["git", "log", "--oneline"],
        cwd=version_dir,
        capture_output=True,
        text=True,
    )
    commit_messages = result.stdout.strip().splitlines()
    assert any("20240102_120000_v2" in msg for msg in commit_messages)
    assert any("20240101_120000_v1" in msg for msg in commit_messages)


def test_build_git_history_skips_if_git_exists(tmp_path: Path):
    version_dir = tmp_path / "versions"
    version_dir.mkdir()
    (version_dir / ".git").mkdir()

    with patch("commands.services.s3_versions.subprocess.run") as mock_run:
        build_git_history(version_dir)
        mock_run.assert_not_called()
