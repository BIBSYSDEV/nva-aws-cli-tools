import base64
import json
import logging
import re
import zlib
from collections import Counter
from pathlib import Path
from typing import Any

import click
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import Binary
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
HANDLE_URL_PREFIX = "https://hdl.handle.net/"
DEFAULT_OWNER_SUBSTRING = "dlr-import-integration"
OWNER_MISMATCH_MARKER = "OWNER-MISMATCH"
OWNER_MISSING_MARKER = "OWNER-MISSING"


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


@files.command("extract-handles")
@click.argument("manifest", type=click.Path(exists=True))
@click.option(
    "--institution",
    default=None,
    help="Comma-separated email domains. Default: all resources in manifest.",
)
@click.option(
    "--output",
    "output_path",
    default=None,
    type=click.Path(),
    help="Write to this file (one handle per line). Default: stdout.",
)
def extract_handles(
    manifest: str,
    institution: str | None,
    output_path: str | None,
) -> None:
    """Extract handles from a manifest as input for `handle redirect-to-nva`.

    Strips the https://hdl.handle.net/ prefix so the output can be piped
    directly into `xargs uv run cli.py handle redirect-to-nva`.
    """
    handles = _extract_handles_from_manifest(manifest, institution)
    text = "\n".join(handles) + ("\n" if handles else "")
    if output_path:
        Path(output_path).write_text(text)
        Console().print(f"Wrote {len(handles)} handles to {output_path}")
    else:
        click.echo(text, nl=False)


def _extract_handles_from_manifest(
    manifest_path: str, institution: str | None
) -> list[str]:
    raw = json.loads(Path(manifest_path).read_text())
    domains = _split_domains(institution) if institution else None
    handles: list[str] = []
    for resource in raw.values():
        if domains is not None and _resource_domain(resource) not in domains:
            continue
        handle = _normalise_handle(resource.get("handle"))
        if handle:
            handles.append(handle)
    return handles


def _normalise_handle(raw_handle: Any) -> str | None:
    if not isinstance(raw_handle, str) or not raw_handle.strip():
        return None
    stripped = raw_handle.strip()
    if stripped.startswith(HANDLE_URL_PREFIX):
        return stripped[len(HANDLE_URL_PREFIX) :]
    return stripped


@files.command("check-source")
@click.argument("manifests", nargs=-1, required=True, type=click.Path(exists=True))
@click.option(
    "--institution",
    default=None,
    help="Comma-separated email domains. Default: all resources in manifest.",
)
@click.option("--detail", is_flag=True, default=False, help="Print every LogEntry row")
@click.option(
    "--require-owner-contains",
    default=DEFAULT_OWNER_SUBSTRING,
    show_default=True,
    help="Flag rows whose Resource.resourceOwner.owner does not contain this string. "
    "Pass empty string to disable.",
)
@click.pass_obj
def check_source(
    ctx: AppContext,
    manifests: tuple[str, ...],
    institution: str | None,
    detail: bool,
    require_owner_contains: str,
) -> None:
    """Read-only: per-resource Query that tallies LogEntry sources.

    One DynamoDB Query per result_id (single partition) — never scans.
    Also fetches the Resource row to surface resourceOwner.owner alongside the
    source counts so you can spot rows that don't belong to the expected importer.
    """
    result_ids = _collect_filtered_result_ids(manifests, institution)
    console = Console()
    console.print(f"Result-ids to check: {len(result_ids)}")

    table = _resolve_resources_table(ctx)
    owner_substring = require_owner_contains or None
    console.print(f"Table: {table.name}  owner_gate={owner_substring or '<disabled>'}")

    grand_total: Counter[str] = Counter()
    resources_with_other = 0
    resources_missing = 0
    resources_owner_mismatch = 0

    for result_id in sorted(result_ids):
        owner = _fetch_resource_owner(table, result_id)
        owner_ok = _owner_matches(owner, owner_substring)
        if not owner_ok:
            resources_owner_mismatch += 1
        rows = _query_log_entries(table, result_id)
        if not rows:
            resources_missing += 1
            console.print(
                f"{result_id}  MISSING (no LogEntry rows)  owner={owner or '<missing>'}"
            )
            continue
        per_resource = Counter(_import_source(row) or "<missing>" for row in rows)
        grand_total.update(per_resource)
        if per_resource.get(SOURCE_OTHER):
            resources_with_other += 1
        owner_tag = "" if owner_ok else f"  [{OWNER_MISMATCH_MARKER}]"
        console.print(
            f"{result_id}  logs={len(rows)}  {_format_sources(per_resource)}  "
            f"owner={owner or '<missing>'}{owner_tag}"
        )
        if detail:
            for row in rows:
                _print_detail_row(console, row)

    console.print("---")
    console.print(f"Resources: {len(result_ids)}  missing: {resources_missing}")
    console.print(f"Resources with any OTHER: {resources_with_other}")
    console.print(f"Resources with owner mismatch: {resources_owner_mismatch}")
    console.print(f"LogEntries total: {sum(grand_total.values())}")
    console.print(f"Sources: {_format_sources(grand_total)}")


@files.command("fix-log-source")
@click.argument("manifests", nargs=-1, required=True, type=click.Path(exists=True))
@click.option(
    "--dry-run/--no-dry-run",
    default=True,
    show_default=True,
    help="Default true; pass --no-dry-run to apply",
)
@click.option(
    "--require-owner-contains",
    default=DEFAULT_OWNER_SUBSTRING,
    show_default=True,
    help="Skip partition if Resource.resourceOwner.owner does not contain this string",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Bypass the resourceOwner check (use with care)",
)
@click.pass_obj
def fix_log_source(
    ctx: AppContext,
    manifests: tuple[str, ...],
    dry_run: bool,
    require_owner_contains: str,
    force: bool,
) -> None:
    """Repair LogEntry rows where data.importSource.source = OTHER → DLR.

    Targets only result_ids found in the supplied manifests (no broad scan).
    By default each partition is gated by a Resource.resourceOwner.owner check
    so that an unrelated OTHER entry cannot be overwritten by accident.
    """
    result_ids = _collect_result_ids(manifests)
    console = Console()
    console.print(f"Result-ids from manifests: {len(result_ids)}")

    table = _resolve_resources_table(ctx)
    owner_substring = None if force else require_owner_contains
    console.print(
        f"Table: {table.name}  dry_run={dry_run}  "
        f"owner_gate={owner_substring or '<disabled>'}"
    )

    updated_count = 0
    failed_count = 0
    candidate_count = 0
    skipped_owner_count = 0
    for result_id in sorted(result_ids):
        if owner_substring is not None:
            owner = _fetch_resource_owner(table, result_id)
            if not _owner_matches(owner, owner_substring):
                skipped_owner_count += 1
                console.print(
                    f"SKIP {result_id} owner={owner or '<missing>'} "
                    f"({OWNER_MISMATCH_MARKER if owner else OWNER_MISSING_MARKER})"
                )
                continue
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
        f"failed={failed_count} skipped_owner={skipped_owner_count} "
        f"dry_run={dry_run}"
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


def _collect_filtered_result_ids(
    manifest_paths: tuple[str, ...], institution: str | None
) -> set[str]:
    if institution is None:
        return _collect_result_ids(manifest_paths)
    domains = _split_domains(institution)
    ids: set[str] = set()
    for path in manifest_paths:
        for result_id, _resource in _load_manifest_for_domains(path, domains):
            ids.add(result_id)
    return ids


def _format_sources(counter: Counter[str]) -> str:
    return "  ".join(f"{name}={count}" for name, count in sorted(counter.items()))


def _print_detail_row(console: Console, row: dict) -> None:
    log_entry_id = _log_entry_identifier(row.get("SK0", ""))
    data = row.get("data", {})
    topic = data.get("topic", "?")
    log_type = data.get("type", "?")
    source = _import_source(row) or "<missing>"
    console.print(
        f"    log={log_entry_id}  type={log_type}  topic={topic}  source={source}"
    )


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


def _fetch_resource_owner(table: Any, result_id: str) -> str | None:
    """GetItem the Resource row and pull resourceOwner.owner out of the zlib data blob.

    Returns None if the row is missing or the owner field can't be extracted.
    """
    key_value = f"{RESOURCE_PK_PREFIX}{result_id}"
    response = table.get_item(Key={"PK0": key_value, "SK0": key_value})
    item = response.get("Item")
    if not item:
        return None
    data = _inflate_resource_data(item.get("data"))
    if not isinstance(data, dict):
        return None
    resource_owner = data.get("resourceOwner")
    if not isinstance(resource_owner, dict):
        return None
    owner = resource_owner.get("owner")
    return owner if isinstance(owner, str) else None


def _inflate_resource_data(blob: Any) -> dict | None:
    if isinstance(blob, dict):
        return blob
    if isinstance(blob, Binary):
        raw = bytes(blob)
    elif isinstance(blob, bytes):
        raw = blob
    elif isinstance(blob, str):
        try:
            raw = base64.b64decode(blob)
        except Exception:
            return None
    else:
        return None
    try:
        inflated = zlib.decompress(raw, -zlib.MAX_WBITS)
        return json.loads(inflated.decode("utf-8"))
    except Exception as error:
        logger.warning("Failed to inflate Resource data: %s", error)
        return None


def _owner_matches(owner: str | None, required_substring: str | None) -> bool:
    if not required_substring:
        return True
    if owner is None:
        return False
    return required_substring in owner


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
