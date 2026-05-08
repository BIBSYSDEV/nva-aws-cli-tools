import gzip
import json
import os
import re
import subprocess
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

ILLEGAL_CHARS = re.compile(r"[^a-zA-Z0-9_-]")

_GIT_IDENTITY_DEFAULTS = {
    "GIT_AUTHOR_NAME": "nva-aws-cli-tools",
    "GIT_AUTHOR_EMAIL": "nva-aws-cli-tools@local",
    "GIT_COMMITTER_NAME": "nva-aws-cli-tools",
    "GIT_COMMITTER_EMAIL": "nva-aws-cli-tools@local",
}


def _git(cwd: Path, *args: str) -> None:
    env = {
        **os.environ,
        **{k: v for k, v in _GIT_IDENTITY_DEFAULTS.items() if k not in os.environ},
    }
    try:
        subprocess.run(
            ["git", *args], cwd=cwd, check=True, capture_output=True, env=env
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(exc.stderr.decode(errors="replace").strip()) from exc


def sanitize_to_folder_name(path: str) -> str:
    return ILLEGAL_CHARS.sub("_", path).strip("_")


def _tracked_filename_for_key(key: str) -> str:
    name = Path(key).name
    if name.endswith(".gz"):
        name = name[:-3]
    if "." not in name:
        name = f"{name}.json"
    return name


def find_bucket(s3_client: Any, name_substring: str) -> str:
    response = s3_client.list_buckets()
    matches = [b["Name"] for b in response["Buckets"] if name_substring in b["Name"]]
    if not matches:
        raise ValueError(f"No bucket found matching '{name_substring}'")
    if len(matches) > 1:
        raise ValueError(f"Multiple buckets match '{name_substring}': {matches}")
    return matches[0]


def fetch_versions(s3_client: Any, bucket: str, key: str) -> list[dict]:
    versions = []
    paginator = s3_client.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket, Prefix=key):
        for version in page.get("Versions", []):
            if version["Key"] == key:
                versions.append(version)
    versions.sort(key=lambda v: v["LastModified"])
    return versions


def decompress_if_needed(data: bytes, key: str) -> bytes:
    if key.endswith(".gz"):
        try:
            return gzip.decompress(data)
        except Exception as exc:
            logger.warning("Failed to decompress gz data: %s", exc)
    return data


def try_pretty_json(data: bytes) -> bytes:
    try:
        parsed = json.loads(data)
        return json.dumps(parsed, indent=2, ensure_ascii=False).encode("utf-8")
    except Exception:
        return data


def download_versions(
    s3_client: Any,
    bucket: str,
    object_path: str,
    output_base: str,
) -> Path:
    key = object_path.lstrip("/")  # tolerate a leading slash from the user
    folder_name = sanitize_to_folder_name(key)
    output_dir = Path(output_base) / folder_name
    output_dir.mkdir(parents=True, exist_ok=True)

    versions = fetch_versions(s3_client, bucket, key)
    if not versions:
        raise ValueError(f"No versions found for '{key}' in bucket '{bucket}'")

    logger.info("Found %d versions in s3://%s/%s", len(versions), bucket, key)

    for version in versions:
        version_id = version["VersionId"]
        last_modified = version["LastModified"]
        date_prefix = last_modified.strftime("%Y%m%d_%H%M%S")
        filename = f"{date_prefix}_{version_id}"
        filepath = output_dir / filename

        if filepath.exists():
            logger.info("Skipping (already exists): %s", filename)
            continue

        response = s3_client.get_object(Bucket=bucket, Key=key, VersionId=version_id)
        raw = response["Body"].read()
        data = decompress_if_needed(raw, key)
        data = try_pretty_json(data)
        filepath.write_bytes(data)
        logger.info("Downloaded: %s", filename)

    return output_dir


def build_git_history(output_dir: Path, key: str = "") -> None:
    tracked_filename = _tracked_filename_for_key(key) if key else "object.json"
    git_dir = output_dir / ".git"
    if git_dir.exists():
        logger.warning(
            "Git repo already exists in %s — skipping. Delete the .git folder to rebuild.",
            output_dir,
        )
        return

    _git(output_dir, "init")
    # Only track the single object file — each commit overwrites it with one version so `git diff` shows changes between versions.
    # The raw version files stay on disk for reference but are intentionally excluded from git.
    (output_dir / ".gitignore").write_text(f"*\n!{tracked_filename}\n!.gitignore\n")
    _git(output_dir, "add", ".gitignore")
    _git(output_dir, "commit", "-m", "init")

    version_files = sorted(
        f for f in output_dir.iterdir() if f.is_file() and not f.name.startswith(".")
    )

    object_file = output_dir / tracked_filename
    for filepath in version_files:
        object_file.write_bytes(filepath.read_bytes())
        _git(output_dir, "add", tracked_filename)
        _git(output_dir, "commit", "-m", filepath.name)

    logger.info(
        "Git history created with %d commits in %s", len(version_files), output_dir
    )
