import json
import logging
import re
from pathlib import Path
from typing import Any

import click
from boto3.dynamodb.conditions import Key
from rich.console import Console

from commands.services.file_upload_api import (
    FILE_TYPE_OPEN,
    PUBLISHER_VERSION_ACCEPTED,
    ExternalClientToken,
    FileUploadApiService,
    S3ObjectSource,
    resolve_api_domain,
)
from commands.utils import AppContext

logger = logging.getLogger(__name__)

LOKE_BUCKET = "loke.storage"
RESOURCES_TABLE_SUBSTRING = "resources"
LOG_ENTRY_SK_PREFIX = "LogEntry:"
RESOURCE_PK_PREFIX = "Resource:"
SOURCE_OTHER = "OTHER"
SOURCE_DLR = "DLR"
CONTENT_TYPE_FILE = "file"
GENERATED_TRUE = "true"


@click.group()
@click.pass_obj
def files(ctx: AppContext) -> None:
    """Transfer DLR files into NVA drafts, publish them, and repair log sources."""
    pass


@files.command("upload-one")
@click.option("--key-file", required=True, type=click.Path(exists=True))
@click.option("--publication", "publication_identifier", required=True)
@click.option("--s3-key", "s3_key", required=True, help="Object key in loke.storage")
@click.option("--bucket", default=LOKE_BUCKET, show_default=True)
@click.option("--filename", default=None, help="Override filename sent to NVA")
@click.option("--mimetype", default=None, help="Override mimetype sent to NVA")
@click.option("--license", "license_uri", default=None)
@click.option(
    "--publisher-version",
    default=PUBLISHER_VERSION_ACCEPTED,
    show_default=True,
    help="Set to empty string to omit",
)
@click.pass_obj
def upload_one(
    ctx: AppContext,
    key_file: str,
    publication_identifier: str,
    s3_key: str,
    bucket: str,
    filename: str | None,
    mimetype: str | None,
    license_uri: str | None,
    publisher_version: str,
) -> None:
    """Upload a single S3 object to one publication. Useful for smoke-tests."""
    service = _build_service(ctx, key_file)
    source = S3ObjectSource(
        ctx.session.client("s3"),
        bucket,
        s3_key,
        filename_override=filename,
        mimetype_override=mimetype,
    )
    result = service.upload(
        publication_identifier,
        source,
        file_type=FILE_TYPE_OPEN,
        license=license_uri,
        publisher_version=publisher_version or None,
    )
    Console().print(result)


@files.command("publish-one")
@click.option("--key-file", required=True, type=click.Path(exists=True))
@click.option("--publication", "publication_identifier", required=True)
@click.pass_obj
def publish_one(
    ctx: AppContext,
    key_file: str,
    publication_identifier: str,
) -> None:
    """Publish a single draft. Idempotent (409 = already published = OK)."""
    service = _build_service(ctx, key_file)
    service.publish(publication_identifier)
    Console().print(f"OK   published {publication_identifier}")


@files.command("upload-manifest")
@click.argument("manifest", type=click.Path(exists=True))
@click.option("--key-file", required=True, type=click.Path(exists=True))
@click.option(
    "--institution",
    required=True,
    help="Comma-separated email domains (e.g. 'ntnu.no,hist.no')",
)
@click.option(
    "--state",
    "state_path",
    default=None,
    help="JSONL file of (result_id, s3_key) pairs to skip and append to",
)
@click.option("--bucket", default=LOKE_BUCKET, show_default=True)
@click.option(
    "--publisher-version",
    default=PUBLISHER_VERSION_ACCEPTED,
    show_default=True,
    help="Set to empty string to omit",
)
@click.option("--dry-run", is_flag=True, default=False)
@click.pass_obj
def upload_manifest(
    ctx: AppContext,
    manifest: str,
    key_file: str,
    institution: str,
    state_path: str | None,
    bucket: str,
    publisher_version: str,
    dry_run: bool,
) -> None:
    """Upload all DLR files for one institution from a data_to_keep manifest."""
    domains = _split_domains(institution)
    items = _load_manifest_for_domains(manifest, domains)
    planned = _plan_uploads(items)
    console = Console()
    console.print(
        f"Resources: {len(items)}  Files to upload: {len(planned)}  "
        f"Domains: {','.join(domains)}"
    )

    if dry_run:
        for result_id, content, _resource in planned:
            console.print(
                f"DRY-RUN {result_id} ← {content['dlr_content_identifier']} "
                f"({content.get('dlr_content', '?')})"
            )
        return

    done = _load_state(state_path)
    service = _build_service(ctx, key_file)
    s3_client = ctx.session.client("s3")

    for result_id, content, resource in planned:
        s3_key = content["dlr_content_identifier"]
        marker = (result_id, s3_key)
        if marker in done:
            console.print(f"SKIP {result_id} ← {s3_key} (already done)")
            continue
        try:
            _upload_one_content(
                service,
                s3_client,
                bucket,
                result_id,
                content,
                resource,
                publisher_version,
            )
            _append_state(state_path, result_id, s3_key, "ok")
            console.print(f"OK   {result_id} ← {s3_key}")
        except Exception as exc:
            _append_state(state_path, result_id, s3_key, f"failed: {exc}")
            console.print(f"FAIL {result_id} ← {s3_key}: {exc}")


@files.command("publish-manifest")
@click.argument("manifest", type=click.Path(exists=True))
@click.option("--key-file", required=True, type=click.Path(exists=True))
@click.option(
    "--institution",
    required=True,
    help="Comma-separated email domains (e.g. 'ntnu.no,hist.no')",
)
@click.option("--dry-run", is_flag=True, default=False)
@click.pass_obj
def publish_manifest(
    ctx: AppContext,
    manifest: str,
    key_file: str,
    institution: str,
    dry_run: bool,
) -> None:
    """Publish all resources for one institution from a data_to_keep manifest."""
    domains = _split_domains(institution)
    items = _load_manifest_for_domains(manifest, domains)
    console = Console()
    console.print(f"Resources to publish: {len(items)}")

    if dry_run:
        for result_id, _resource in items:
            console.print(f"DRY-RUN publish {result_id}")
        return

    service = _build_service(ctx, key_file)
    for result_id, _resource in items:
        try:
            service.publish(result_id)
            console.print(f"OK   published {result_id}")
        except Exception as exc:
            console.print(f"FAIL publish {result_id}: {exc}")


@files.command("fix-log-source")
@click.argument("manifests", nargs=-1, required=True, type=click.Path(exists=True))
@click.option(
    "--dry-run/--no-dry-run",
    default=True,
    show_default=True,
    help="Default true; pass --no-dry-run to apply",
)
@click.pass_obj
def fix_log_source(
    ctx: AppContext,
    manifests: tuple[str, ...],
    dry_run: bool,
) -> None:
    """Repair LogEntry rows where data.importSource.source = OTHER → DLR.

    Targets only result_ids found in the supplied manifests (no broad scan).
    """
    result_ids = _collect_result_ids(manifests)
    console = Console()
    console.print(f"Result-ids from manifests: {len(result_ids)}")

    table = _resolve_resources_table(ctx)
    console.print(f"Table: {table.name}  dry_run={dry_run}")

    updated_count = 0
    failed_count = 0
    candidate_count = 0
    for result_id in sorted(result_ids):
        rows = _query_log_entries(table, result_id)
        for row in rows:
            if _import_source(row) != SOURCE_OTHER:
                continue
            candidate_count += 1
            log_entry_id = _log_entry_identifier(row.get("SK0", ""))
            topic = row.get("data", {}).get("topic", "?")
            console.print(
                f"{'DRY-RUN' if dry_run else 'UPDATE'} {result_id} "
                f"log={log_entry_id} topic={topic} OTHER→DLR"
            )
            if not dry_run:
                try:
                    _update_log_source_to_dlr(table, row)
                    updated_count += 1
                except Exception as exc:
                    failed_count += 1
                    console.print(f"FAIL {result_id} log={log_entry_id}: {exc}")

    console.print(
        f"Done. candidates={candidate_count} updated={updated_count} "
        f"failed={failed_count} dry_run={dry_run}"
    )


def _build_service(ctx: AppContext, key_file: str) -> FileUploadApiService:
    token = ExternalClientToken.from_key_file(key_file)
    api_domain = resolve_api_domain(ctx.session)
    return FileUploadApiService(api_domain=api_domain, token=token)


def _split_domains(institution: str) -> list[str]:
    return [part.strip().lower() for part in institution.split(",") if part.strip()]


def _load_manifest_for_domains(
    manifest_path: str, domains: list[str]
) -> list[tuple[str, dict]]:
    raw = json.loads(Path(manifest_path).read_text())
    selected = []
    for _dlr_id, resource in raw.items():
        result_id = resource.get("result_id")
        if not result_id:
            continue
        if _resource_domain(resource) in domains:
            selected.append((result_id, resource))
    return selected


def _resource_domain(resource: dict) -> str | None:
    for content in resource.get("content", []):
        email = content.get("dlr_submitter_email")
        if email and "@" in email:
            return email.split("@", 1)[1].strip().lower()
    return None


def _plan_uploads(
    items: list[tuple[str, dict]],
) -> list[tuple[str, dict, dict]]:
    planned = []
    for result_id, resource in items:
        for content in resource.get("content", []):
            if _is_uploadable_content(content):
                planned.append((result_id, content, resource))
    return planned


def _is_uploadable_content(content: dict) -> bool:
    if content.get("dlr_content_type") != CONTENT_TYPE_FILE:
        return False
    return content.get("dlr_content_generated") != GENERATED_TRUE


def _upload_one_content(
    service: FileUploadApiService,
    s3_client: Any,
    bucket: str,
    result_id: str,
    content: dict,
    resource: dict,
    publisher_version: str,
) -> None:
    source = S3ObjectSource(
        s3_client,
        bucket,
        content["dlr_content_identifier"],
        filename_override=content.get("dlr_content"),
        mimetype_override=content.get("dlr_content_mime_type"),
    )
    service.upload(
        result_id,
        source,
        file_type=FILE_TYPE_OPEN,
        license=resource.get("license"),
        publisher_version=publisher_version or None,
    )


def _load_state(state_path: str | None) -> set[tuple[str, str]]:
    if state_path is None:
        return set()
    path = Path(state_path)
    if not path.exists():
        return set()
    done: set[tuple[str, str]] = set()
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        if entry.get("status", "").startswith("ok"):
            done.add((entry["result_id"], entry["s3_key"]))
    return done


def _append_state(
    state_path: str | None, result_id: str, s3_key: str, status: str
) -> None:
    if state_path is None:
        return
    with open(state_path, "a") as state_file:
        state_file.write(
            json.dumps({"result_id": result_id, "s3_key": s3_key, "status": status})
            + "\n"
        )


def _collect_result_ids(manifest_paths: tuple[str, ...]) -> set[str]:
    ids: set[str] = set()
    for path in manifest_paths:
        raw = json.loads(Path(path).read_text())
        for resource in raw.values():
            result_id = resource.get("result_id")
            if result_id:
                ids.add(result_id)
    return ids


def _resolve_resources_table(ctx: AppContext) -> Any:
    dynamodb = ctx.session.client("dynamodb")
    response = dynamodb.list_tables()
    candidates = [
        name for name in response["TableNames"] if RESOURCES_TABLE_SUBSTRING in name
    ]
    if not candidates:
        raise click.ClickException(
            f"No DynamoDB table containing '{RESOURCES_TABLE_SUBSTRING}' found"
        )
    if len(candidates) > 1:
        raise click.ClickException(
            f"Ambiguous tables matching '{RESOURCES_TABLE_SUBSTRING}': {candidates}"
        )
    return ctx.session.resource("dynamodb").Table(candidates[0])


def _query_log_entries(table: Any, result_id: str) -> list[dict]:
    response = table.query(
        KeyConditionExpression=Key("PK0").eq(f"{RESOURCE_PK_PREFIX}{result_id}")
        & Key("SK0").begins_with(LOG_ENTRY_SK_PREFIX),
    )
    rows = list(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = table.query(
            KeyConditionExpression=Key("PK0").eq(f"{RESOURCE_PK_PREFIX}{result_id}")
            & Key("SK0").begins_with(LOG_ENTRY_SK_PREFIX),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        rows.extend(response.get("Items", []))
    return rows


def _import_source(row: dict) -> str | None:
    data = row.get("data")
    if not isinstance(data, dict):
        return None
    import_source = data.get("importSource")
    if not isinstance(import_source, dict):
        return None
    return import_source.get("source")


def _log_entry_identifier(sort_key: str) -> str:
    match = re.match(rf"^{re.escape(LOG_ENTRY_SK_PREFIX)}(.+)$", sort_key)
    return match.group(1) if match else sort_key


def _update_log_source_to_dlr(table: Any, row: dict) -> None:
    table.update_item(
        Key={"PK0": row["PK0"], "SK0": row["SK0"]},
        UpdateExpression="SET #data.#importSource.#source = :dlr",
        ConditionExpression="#data.#importSource.#source = :other",
        ExpressionAttributeNames={
            "#data": "data",
            "#importSource": "importSource",
            "#source": "source",
        },
        ExpressionAttributeValues={":dlr": SOURCE_DLR, ":other": SOURCE_OTHER},
    )
