import click
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from commands.services.search_api import SearchApiService
from commands.utils import AppContext
from log_config import log_console

logger = logging.getLogger(__name__)


@dataclass
class SearchParams:
    aggregation: str | None = None
    year_to: str | None = None
    year_from: str | None = None
    unit: str | None = None
    publisher: str | None = None
    contributor: str | None = None
    project: str | None = None
    funding_source: str | None = None
    funding_identifier: str | None = None
    category: str | None = None
    instance_type: str | None = None
    order: str | None = None
    sort: str = "relevance,identifier"

    PARAM_MAPPING = {
        "aggregation": "aggregation",
        "year_to": "publicationYearBefore",
        "year_from": "publicationYearSince",
        "unit": "unit",
        "publisher": "publisher",
        "contributor": "contributor",
        "project": "project",
        "funding_source": "fundingSource",
        "funding_identifier": "fundingIdentifier",
        "category": "category",
        "instance_type": "instanceType",
        "order": "order",
        "sort": "sort",
    }

    def to_query_params(self, api_domain: str) -> dict:
        query_params = {}

        for field_name, api_name in self.PARAM_MAPPING.items():
            value = getattr(self, field_name)
            if value:
                query_params[api_name] = value

        if "unit" in query_params and not query_params["unit"].startswith("http"):
            unit_id = query_params["unit"]
            query_params["unit"] = (
                f"https://{api_domain}/cristin/organization/{unit_id}"
            )

        if "project" in query_params and not query_params["project"].startswith("http"):
            project_id = query_params["project"]
            query_params["project"] = (
                f"https://{api_domain}/cristin/project/{project_id}"
            )

        return query_params

    @classmethod
    def from_kwargs(cls, **kwargs) -> "SearchParams":
        field_names = {f for f in cls.PARAM_MAPPING.keys()}
        filtered = {k: v for k, v in kwargs.items() if k in field_names}
        return cls(**filtered)


@click.group()
@click.pass_obj
def search(ctx: AppContext):
    """Search NVA resources."""
    pass


@search.command()
@click.pass_obj
@click.option(
    "--page-size",
    type=int,
    default=500,
    help="Maximum results per page. The client starts low, ramps up toward this "
    "while pages succeed, and backs off (down to 1) when the server fails a page "
    "(default: 500)",
)
@click.option(
    "--limit",
    type=int,
    help="Maximum number of total results to return (default: unlimited)",
)
@click.option(
    "--aggregation",
    type=str,
    default="none",
    help="Aggregation parameter (default: none)",
)
@click.option(
    "--year-to",
    type=str,
    help="Publication year before (e.g., 2026)",
)
@click.option(
    "--year-from",
    type=str,
    help="Publication year since (e.g., 2025)",
)
@click.option(
    "--unit",
    type=str,
    help="Unit/organization ID (e.g., 1965.0.0.0)",
)
@click.option(
    "--publisher",
    type=str,
    help="Publisher UUID",
)
@click.option(
    "--contributor",
    type=str,
    help="Contributor ID",
)
@click.option(
    "--project",
    type=str,
    help="Project ID (e.g., 2744839 or full URL)",
)
@click.option(
    "--funding-source",
    type=str,
    help="Funding source (e.g., NFR)",
)
@click.option(
    "--funding-identifier",
    type=str,
    help="Funding identifier (e.g., 357438)",
)
@click.option(
    "--category",
    type=str,
    help="Category filter",
)
@click.option(
    "--instance-type",
    type=str,
    help="Instance type filter",
)
@click.option(
    "--order",
    type=str,
    help="Order field (e.g., modifiedDate, createdDate). For large exports prefer "
    "an immutable field like createdDate; modifiedDate can cause updated records "
    "to be skipped (see the command help for details)",
)
@click.option(
    "--sort",
    type=str,
    default="relevance,identifier",
    help="Sort order (default: relevance,identifier)",
)
@click.option(
    "--id-only",
    is_flag=True,
    help="Output only identifiers (one per line)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, writable=True),
    help="Base path for JSONL output; a zero-padded batch number is inserted before the extension (e.g. out.jsonl -> out_00001.jsonl). Lines are JSON objects, or bare identifiers with --id-only. Missing directories are created",
)
@click.option(
    "--batch-size",
    type=click.IntRange(min=1),
    default=1000,
    show_default=True,
    help="When writing to --output, split into files of this many records (a zero-padded counter is inserted before the extension)",
)
@click.option(
    "--query",
    type=str,
    multiple=True,
    help="Additional query parameters in format key=value (can be used multiple times)",
)
@click.option(
    "--exclude-fields",
    type=str,
    multiple=True,
    help="Top-level field(s) to drop from each hit (maps to nodesExcluded). "
    "Comma-separated or repeated, e.g. --exclude-fields contributorsPreview,tags",
)
@click.option(
    "--api-version",
    type=str,
    default="2024-12-01",
    help="API version for the Accept header (default: 2024-12-01). Pass an empty "
    'string (--api-version "") to omit the version and use the server default',
)
def resources(
    ctx: AppContext,
    page_size: int,
    limit: int | None,
    id_only: bool,
    output: str | None,
    batch_size: int,
    query: Tuple[str, ...],
    exclude_fields: Tuple[str, ...],
    api_version: str,
    **kwargs,
) -> None:
    """Search NVA resources with flexible query parameters.

    This command provides automatic pagination and supports all search API parameters.

    Stable pagination (avoiding skipped records): search-after is only stable when
    the sort key never changes for a document while you page. Sorting on
    --order modifiedDate is unsafe for large exports, because an updated document
    gets a new modifiedDate and moves past the cursor, so it is skipped. Prefer an
    immutable field: --order createdDate never changes after creation, so existing
    documents are never lost and new ones land at the end (picked up with
    --sort asc). The API appends the unique 'identifier' as a tie-breaker, so equal
    sort values between pages are already handled; duplicates may appear (e.g. docs
    created during the run) but no existing record is dropped.

    \b
    Recommended for a full export:
    uv run cli.py search resources --order createdDate --sort asc --output out.jsonl

    \b
    Examples:
        # Search by unit and year range
        uv run cli.py search resources --unit 1965.0.0.0 --year-from 2025 --year-to 2026
    \b
        # Search by project (limit to first 100 results)
        uv run cli.py search resources --project 2744839 --limit 100
    \b
        # Search by funding source and identifier
        uv run cli.py search resources --funding-source NFR --funding-identifier 357438
    \b
        # Search by publisher (output only IDs)
        uv run cli.py search resources --publisher 08DC24C9-B7FF-4192-89AC-C629D93AD9CF --id-only
    \b
        # Write results to JSONL files, split into batches of 1000 (default)
        uv run cli.py search resources --unit 194.0.0.0 --output resultat.jsonl
        # -> resultat_00001.jsonl, resultat_00002.jsonl, ...
    \b
        # Override the batch size
        uv run cli.py search resources --unit 194.0.0.0 --output resultat.jsonl --batch-size 500
    \b
        # Use additional query parameters
        uv run cli.py search resources --query "funding=some-id" --query "status=published" --aggregation all
    \b
        # Drop heavy fields from each hit (nodesExcluded)
        uv run cli.py search resources --exclude-fields contributorsPreview,tags --output out.jsonl
    """
    search_service = SearchApiService(session=ctx.session)
    search_params = SearchParams.from_kwargs(**kwargs)
    query_params = search_params.to_query_params(search_service.api_domain)

    for q in query:
        if "=" in q:
            key, value = q.split("=", 1)
            query_params[key] = value
        else:
            logger.warning(f"Ignoring invalid query parameter: {q}")

    excluded = _split_csv(exclude_fields)
    if excluded:
        query_params["nodesExcluded"] = ",".join(excluded)

    compact = output is not None

    try:
        with Progress(*_progress_columns(), console=log_console) as progress:
            task_id = None

            def start_progress(total_hits: int) -> None:
                nonlocal task_id
                capped_total = min(total_hits, limit) if limit else total_hits
                task_id = progress.add_task("Fetching", total=capped_total)

            with _JsonlSink(output, batch_size) as sink:
                count = 0
                for hit in search_service.resource_search(
                    query_params,
                    page_size,
                    api_version=api_version,
                    on_total_hits=start_progress,
                ):
                    if task_id is not None:
                        progress.advance(task_id)

                    line = _format_hit_line(hit, id_only, compact)
                    if line is not None:
                        sink.write(line)
                        count += 1

                    if limit and count >= limit:
                        break

                if count > 0:
                    _log_result_summary(count, output, batch_size, sink.paths)

    except Exception as e:
        logger.error(f"Error fetching resources: {e}")
        raise click.Abort()


def _split_csv(values: Tuple[str, ...]) -> list:
    items = []
    for value in values:
        items.extend(part.strip() for part in value.split(",") if part.strip())
    return items


def _progress_columns() -> tuple:
    return (
        TextColumn("[progress.description]{task.description}"),
        BarColumn(complete_style="green", finished_style="green"),
        MofNCompleteColumn(),
        TextColumn("hits • elapsed"),
        TimeElapsedColumn(),
        TextColumn("• remaining"),
        TimeRemainingColumn(),
    )


def _format_hit_line(hit: dict, id_only: bool, compact: bool) -> str | None:
    if id_only:
        return hit.get("identifier")
    if compact:
        return json.dumps(hit)
    return json.dumps(hit, indent=2)


def _log_result_summary(
    count: int, output: str | None, batch_size: int, paths: list
) -> None:
    if output is None:
        logger.info(f"Total results: {count}")
    elif len(paths) == 1:
        logger.info(f"Wrote {count} results to {paths[0]}")
    else:
        logger.info(
            f"Wrote {count} results to {len(paths)} files "
            f"({paths[0]} … {paths[-1]}, {batch_size} per file)"
        )


class _JsonlSink:
    def __init__(self, output: str | None, batch_size: int) -> None:
        self._output = output
        self._batch_size = batch_size
        self._file = None
        self._paths = []
        self._lines_in_batch = 0

    def __enter__(self) -> "_JsonlSink":
        return self

    def __exit__(self, *exc_info) -> bool:
        self._close_current_file()
        return False

    @property
    def paths(self) -> list:
        return self._paths

    @property
    def file_count(self) -> int:
        return len(self._paths)

    def write(self, line: str) -> None:
        if self._output is None:
            print(line)
            return

        if self._file is None or self._lines_in_batch >= self._batch_size:
            self._open_next_file()

        self._file.write(line)
        self._file.write("\n")
        self._lines_in_batch += 1

    def _open_next_file(self) -> None:
        self._close_current_file()
        path = self._batch_path()
        self._paths.append(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._file = open(path, "w", encoding="utf-8")
        self._lines_in_batch = 0

    def _batch_path(self) -> str:
        next_index = len(self._paths) + 1
        path = Path(self._output)
        return str(path.with_name(f"{path.stem}_{next_index:05d}{path.suffix}"))

    def _close_current_file(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
