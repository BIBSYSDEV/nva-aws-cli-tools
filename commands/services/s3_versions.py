import boto3
import gzip
import json
import re
import subprocess
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

ILLEGAL_CHARS = re.compile(r"[^a-zA-Z0-9_-]")


def sanitize_to_folder_name(path: str) -> str:
    return ILLEGAL_CHARS.sub("_", path).strip("_")


def fetch_versions(s3_client, bucket: str, key: str) -> list[dict]:
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
            logger.warning(f"Failed to decompress gz data: {exc}")
    return data


def try_pretty_json(data: bytes) -> bytes:
    try:
        parsed = json.loads(data)
        return json.dumps(parsed, indent=2, ensure_ascii=False).encode("utf-8")
    except Exception:
        return data


def download_versions(
    s3_client,
    bucket: str,
    object_path: str,
    output_base: str,
) -> Path:
    key = object_path.lstrip("/")  # tolerate a leading slash from the user
    folder_name = sanitize_to_folder_name(object_path)
    output_dir = Path(output_base) / folder_name
    output_dir.mkdir(parents=True, exist_ok=True)

    versions = fetch_versions(s3_client, bucket, key)
    if not versions:
        raise ValueError(f"No versions found for '{key}' in bucket '{bucket}'")

    logger.info(f"Found {len(versions)} versions in s3://{bucket}/{key}")

    for version in versions:
        version_id = version["VersionId"]
        last_modified = version["LastModified"]
        date_prefix = last_modified.strftime("%Y%m%d_%H%M%S")
        filename = f"{date_prefix}_{version_id}"
        filepath = output_dir / filename

        if filepath.exists():
            logger.info(f"Skipping (already exists): {filename}")
            continue

        response = s3_client.get_object(Bucket=bucket, Key=key, VersionId=version_id)
        raw = response["Body"].read()
        data = decompress_if_needed(raw, key)
        data = try_pretty_json(data)
        filepath.write_bytes(data)
        logger.info(f"Downloaded: {filename}")

    return output_dir


def build_git_history(output_dir: Path) -> None:
    git_dir = output_dir / ".git"
    if git_dir.exists():
        logger.info("Git repo already exists, skipping git history creation")
        return

    subprocess.run(["git", "init"], cwd=output_dir, check=True)
    (output_dir / ".gitignore").write_text("*\n!object.json\n!.gitignore\n")
    subprocess.run(["git", "add", ".gitignore"], cwd=output_dir, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=output_dir, check=True)

    version_files = sorted(
        f for f in output_dir.iterdir() if f.is_file() and not f.name.startswith(".")
    )

    object_file = output_dir / "object.json"
    for filepath in version_files:
        object_file.write_bytes(filepath.read_bytes())
        subprocess.run(["git", "add", "object.json"], cwd=output_dir, check=True)
        subprocess.run(
            ["git", "commit", "--allow-empty", "-m", filepath.name],
            cwd=output_dir,
            check=True,
        )

    logger.info(f"Git history created with {len(version_files)} commits in {output_dir}")
