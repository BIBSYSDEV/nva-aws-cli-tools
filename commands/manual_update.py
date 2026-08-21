import functools
import json
import logging
import os
import signal
import sys
from datetime import UTC, datetime

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from commands.services.entity_lookup import EntityResolver
from commands.services.manual_update_api import (
    DEFAULT_LIMIT,
    DEFAULT_PAGE_SIZE,
    Comparator,
    ManualUpdateError,
    ManualUpdateRequest,
    ManualUpdateService,
    ManualUpdateType,
)
from commands.utils import AppContext

logger = logging.getLogger(__name__)

COMPARATOR_CHOICES = [comparator.value for comparator in Comparator]
TYPE_CHOICES = [update_type.value for update_type in ManualUpdateType]

QUERY_UNCONFIRMED_PUBLISHER = "UnconfirmedPublisher"
CHANGE_LOG_DIR = "manual-update-logs"
HEAD_TAIL_COUNT = 5


def _handle_service_errors(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ManualUpdateError as error:
            raise click.ClickException(str(error))

    return wrapper


def _shared_options(func):
    func = click.option(
        "--search",
        "search_pairs",
        multiple=True,
        metavar="KEY=VALUE",
        help="Extra search parameter (repeatable). Overrides auto-built parameters.",
    )(func)
    func = click.option(
        "--limit",
        type=int,
        default=None,
        help="Max number of resources to change before stopping (default 10000).",
    )(func)
    func = click.option(
        "--page-size",
        type=int,
        default=None,
        help="Search page size, 1-1000 (default 100).",
    )(func)
    func = click.option(
        "--yes", is_flag=True, default=False, help="Skip the confirmation prompt."
    )(func)
    func = click.option(
        "--dry-run-only",
        is_flag=True,
        default=False,
        help="Only preview the changes, never apply them.",
    )(func)
    return func


@click.group(name="manual-update")
@click.pass_obj
def manual_update(ctx: AppContext):
    """Batch-update published metadata via the ManuallyUpdatePublications Lambda.

    Every command previews the change with a dry run and asks for confirmation
    before writing anything.
    """


@manual_update.command()
@click.option("--type", "update_type", type=click.Choice(TYPE_CHOICES), required=True)
@click.option("--old", "old_value", required=True, help="Value to replace.")
@click.option("--new", "new_value", required=True, help="Replacement value.")
@click.option(
    "--comparator",
    type=click.Choice(COMPARATOR_CHOICES),
    default=None,
    help="Matching for oldValue on unconfirmed channels.",
)
@_shared_options
@click.pass_obj
@_handle_service_errors
def run(
    ctx: AppContext,
    update_type: str,
    old_value: str,
    new_value: str,
    comparator: str | None,
    search_pairs: tuple[str, ...],
    limit: int | None,
    page_size: int | None,
    yes: bool,
    dry_run_only: bool,
) -> None:
    """Generic update mirroring the request format; covers every update type."""
    search_params = _parse_search(search_pairs)
    if not search_params:
        raise click.UsageError(
            "At least one --search KEY=VALUE is required to narrow the affected resources."
        )
    request = ManualUpdateRequest(
        type=ManualUpdateType(update_type),
        old_value=old_value,
        new_value=new_value,
        search_params=search_params,
        comparator=Comparator(comparator) if comparator else None,
        limit=limit,
        page_size=page_size,
    )
    _execute(ctx, request, yes, dry_run_only)


@manual_update.command()
@click.argument("old_value")
@click.argument("new_value")
@_shared_options
@click.pass_obj
@_handle_service_errors
def publisher(
    ctx: AppContext,
    old_value: str,
    new_value: str,
    search_pairs: tuple[str, ...],
    limit: int | None,
    page_size: int | None,
    yes: bool,
    dry_run_only: bool,
) -> None:
    """Replace publisher id OLD_VALUE with NEW_VALUE."""
    request = _build_request(
        ManualUpdateType.PUBLISHER,
        old_value,
        new_value,
        {"publisher": old_value},
        search_pairs,
        limit,
        page_size,
    )
    resolver = EntityResolver(ctx.session)
    _execute(
        ctx,
        request,
        yes,
        dry_run_only,
        old_label=resolver.publisher_name(old_value),
        new_label=resolver.publisher_name(new_value),
    )


@manual_update.command()
@click.argument("old_value")
@click.argument("new_value")
@_shared_options
@click.pass_obj
@_handle_service_errors
def project(
    ctx: AppContext,
    old_value: str,
    new_value: str,
    search_pairs: tuple[str, ...],
    limit: int | None,
    page_size: int | None,
    yes: bool,
    dry_run_only: bool,
) -> None:
    """Replace project id OLD_VALUE with NEW_VALUE."""
    request = _build_request(
        ManualUpdateType.PROJECT,
        old_value,
        new_value,
        {"project": old_value},
        search_pairs,
        limit,
        page_size,
    )
    resolver = EntityResolver(ctx.session)
    _execute(
        ctx,
        request,
        yes,
        dry_run_only,
        old_label=resolver.project_title(old_value),
        new_label=resolver.project_title(new_value),
    )


@manual_update.command(name="contributor-identifier")
@click.argument("old_value")
@click.argument("new_value")
@_shared_options
@click.pass_obj
@_handle_service_errors
def contributor_identifier(
    ctx: AppContext,
    old_value: str,
    new_value: str,
    search_pairs: tuple[str, ...],
    limit: int | None,
    page_size: int | None,
    yes: bool,
    dry_run_only: bool,
) -> None:
    """Replace contributor id OLD_VALUE with NEW_VALUE."""
    request = _build_request(
        ManualUpdateType.CONTRIBUTOR_IDENTIFIER,
        old_value,
        new_value,
        {"contributor": old_value},
        search_pairs,
        limit,
        page_size,
    )
    resolver = EntityResolver(ctx.session)
    _execute(
        ctx,
        request,
        yes,
        dry_run_only,
        old_label=resolver.person_name(old_value),
        new_label=resolver.person_name(new_value),
    )


@manual_update.command(name="contributor-affiliation")
@click.argument("old_value")
@click.argument("new_value")
@click.option(
    "--unit",
    default=None,
    help="Place code to search by. Defaults to the last path segment of OLD_VALUE.",
)
@_shared_options
@click.pass_obj
@_handle_service_errors
def contributor_affiliation(
    ctx: AppContext,
    old_value: str,
    new_value: str,
    unit: str | None,
    search_pairs: tuple[str, ...],
    limit: int | None,
    page_size: int | None,
    yes: bool,
    dry_run_only: bool,
) -> None:
    """Move contributor affiliation from OLD_VALUE URI to NEW_VALUE URI."""
    request = _build_request(
        ManualUpdateType.CONTRIBUTOR_AFFILIATION,
        old_value,
        new_value,
        {"unit": unit or _last_path_segment(old_value)},
        search_pairs,
        limit,
        page_size,
    )
    resolver = EntityResolver(ctx.session)
    _execute(
        ctx,
        request,
        yes,
        dry_run_only,
        old_label=resolver.organization_label(old_value),
        new_label=resolver.organization_label(new_value),
    )


@manual_update.command(name="unconfirmed-publisher")
@click.argument("old_value")
@click.argument("new_value")
@click.option(
    "--comparator",
    type=click.Choice(COMPARATOR_CHOICES),
    default=Comparator.CONTAINS.value,
    show_default=True,
    help="How to match OLD_VALUE against the stored publisher name.",
)
@_shared_options
@click.pass_obj
@_handle_service_errors
def unconfirmed_publisher(
    ctx: AppContext,
    old_value: str,
    new_value: str,
    comparator: str,
    search_pairs: tuple[str, ...],
    limit: int | None,
    page_size: int | None,
    yes: bool,
    dry_run_only: bool,
) -> None:
    """Convert unconfirmed publisher name OLD_VALUE to verified publisher id NEW_VALUE."""
    request = _build_request(
        ManualUpdateType.UNCONFIRMED_PUBLISHER,
        old_value,
        new_value,
        {"publisher": old_value, "query": QUERY_UNCONFIRMED_PUBLISHER},
        search_pairs,
        limit,
        page_size,
        comparator=Comparator(comparator),
    )
    resolver = EntityResolver(ctx.session)
    _execute(
        ctx,
        request,
        yes,
        dry_run_only,
        new_label=resolver.publisher_name(new_value),
    )


def _build_request(
    update_type: ManualUpdateType,
    old_value: str,
    new_value: str,
    default_search: dict[str, str],
    search_pairs: tuple[str, ...],
    limit: int | None,
    page_size: int | None,
    comparator: Comparator | None = None,
) -> ManualUpdateRequest:
    search_params = {**default_search, **_parse_search(search_pairs)}
    return ManualUpdateRequest(
        type=update_type,
        old_value=old_value,
        new_value=new_value,
        search_params=search_params,
        comparator=comparator,
        limit=limit,
        page_size=page_size,
    )


def _parse_search(pairs: tuple[str, ...]) -> dict[str, str]:
    result: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise click.UsageError(f"--search must be KEY=VALUE, got: {pair}")
        key, value = pair.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def _last_path_segment(uri: str) -> str:
    return uri.rstrip("/").rsplit("/", 1)[-1]


def _execute(
    ctx: AppContext,
    request: ManualUpdateRequest,
    yes: bool,
    dry_run_only: bool,
    old_label: str | None = None,
    new_label: str | None = None,
) -> None:
    service = ManualUpdateService(session=ctx.session)
    console = Console()
    change_line = _describe_change(
        old_label, request.old_value, new_label, request.new_value
    )

    report = service.dry_run(request)
    _render_report(console, report, "DRY RUN", change_line)

    changes = report.get("changes", [])
    if not changes:
        console.print("[yellow]No changes to apply.[/yellow]")
        return
    if _is_truncated(changes):
        log_path = _write_change_log(request, report, "dry-run")
        console.print(f"[dim]Full plan written to {log_path}[/dim]")
    if dry_run_only:
        return

    if not yes:
        _restore_interactive_terminal()
        click.confirm(f"Apply {len(changes)} change(s)?", default=True, abort=True)

    result = service.apply(request)
    _render_report(console, result, "APPLIED", change_line)
    log_path = _write_change_log(request, result, "applied")
    console.print(f"Change log written to {log_path}")
    if result.get("limitReached") and _more_results_pending(result):
        console.print(
            f"[yellow]Stopped at your limit of {result.get('limit')} change(s) with more "
            "results still pending — run again to continue.[/yellow]"
        )


def _is_truncated(changes: list) -> bool:
    return len(changes) > HEAD_TAIL_COUNT * 2


def _write_change_log(request: ManualUpdateRequest, report: dict, phase: str) -> str:
    os.makedirs(CHANGE_LOG_DIR, exist_ok=True)
    now = datetime.now(UTC)
    filename = f"{now.strftime('%Y%m%dT%H%M%SZ')}-{request.type.value}-{phase}.json"
    path = os.path.join(CHANGE_LOG_DIR, filename)
    payload = {
        "timestamp": now.isoformat(),
        "phase": phase,
        "type": report.get("type"),
        "oldValue": report.get("oldValue"),
        "newValue": report.get("newValue"),
        "searchParams": request.search_params,
        "comparator": request.comparator.value if request.comparator else None,
        "totalHits": report.get("totalHits"),
        "resourcesMatched": report.get("resourcesMatched"),
        "resourcesChanged": report.get("resourcesChanged"),
        "limit": report.get("limit"),
        "limitReached": report.get("limitReached"),
        "changes": report.get("changes", []),
    }
    with open(path, "w") as log_file:
        json.dump(payload, log_file, indent=2, ensure_ascii=False)
    return path


def _more_results_pending(report: dict) -> bool:
    total_hits = report.get("totalHits") or 0
    hits_returned = report.get("hitsReturned") or 0
    return hits_returned < total_hits


def _describe_change(
    old_label: str | None,
    old_value: str,
    new_label: str | None,
    new_value: str,
) -> str:
    return f"{_labelled(old_label, old_value)} → {_labelled(new_label, new_value)}"


def _labelled(label: str | None, value: str) -> str:
    return f"{label} ({value})" if label else value


def _restore_interactive_terminal() -> None:
    """Undo any raw/no-signal terminal state left by an earlier step so Ctrl+C aborts."""
    try:
        signal.signal(signal.SIGINT, signal.default_int_handler)
    except Exception as error:  # noqa: BLE001 - not in main thread, or no SIGINT on platform
        logger.debug("Could not restore SIGINT handler: %s", error)
    if not sys.stdin.isatty():
        return
    try:
        import termios

        file_descriptor = sys.stdin.fileno()
        attributes = termios.tcgetattr(file_descriptor)
        attributes[3] |= termios.ICANON | termios.ECHO | termios.ISIG
        attributes[6][termios.VINTR] = b"\x03"
        termios.tcsetattr(file_descriptor, termios.TCSANOW, attributes)
    except Exception as error:  # noqa: BLE001 - restoring the terminal must never crash
        logger.debug("Could not restore terminal mode: %s", error)


def _render_report(
    console: Console, report: dict, phase: str, change_line: str | None = None
) -> None:
    body = change_line or f"{report.get('oldValue')} → {report.get('newValue')}"
    console.print(
        Panel(f"[bold]{phase}[/bold]  {report.get('type')}\n{body}", expand=False)
    )
    _render_summary(console, report)
    _render_changes(console, report.get("changes", []))


def _render_summary(console: Console, report: dict) -> None:
    summary = Table.grid(padding=(0, 2))
    summary.add_column(style="bold cyan")
    summary.add_column()

    summary.add_row("Total hits", str(report.get("totalHits")))
    if report.get("resourcesFetched") != report.get("totalHits"):
        summary.add_row("Resources fetched", str(report.get("resourcesFetched")))
    summary.add_row("Resources matched", str(report.get("resourcesMatched")))
    summary.add_row("Resources changed", str(report.get("resourcesChanged")))
    summary.add_row("Pages fetched", str(report.get("pagesFetched")))

    limit_reached = bool(report.get("limitReached"))
    if report.get("limit") != DEFAULT_LIMIT or limit_reached:
        summary.add_row("Limit", str(report.get("limit")))
    if limit_reached:
        summary.add_row("Limit reached", "True")
    if report.get("pageSize") != DEFAULT_PAGE_SIZE:
        summary.add_row("Page size", str(report.get("pageSize")))

    console.print(summary)


def _render_changes(console: Console, changes: list) -> None:
    if not changes:
        return
    table = Table(show_header=True, header_style="bold cyan", title="Changes")
    table.add_column("Identifier", overflow="fold")
    table.add_column("Field", overflow="fold")
    table.add_column("Old", overflow="fold")
    table.add_column("New", overflow="fold")

    if _is_truncated(changes):
        for change in changes[:HEAD_TAIL_COUNT]:
            _add_change_rows(table, change)
        table.add_row(f"… {len(changes) - 2 * HEAD_TAIL_COUNT} more …", "", "", "")
        for change in changes[-HEAD_TAIL_COUNT:]:
            _add_change_rows(table, change)
    else:
        for change in changes:
            _add_change_rows(table, change)

    console.print(table)
    if _is_truncated(changes):
        console.print(
            f"[dim]Showing first {HEAD_TAIL_COUNT} and last {HEAD_TAIL_COUNT} "
            f"of {len(changes)} changed resources; see the log for the full list.[/dim]"
        )


def _add_change_rows(table: Table, change: dict) -> None:
    identifier = change.get("identifier", "")
    field_changes = change.get("fieldChanges", [])
    for index, field_change in enumerate(field_changes):
        table.add_row(
            identifier if index == 0 else "",
            field_change.get("path", ""),
            field_change.get("oldValue", ""),
            field_change.get("newValue", ""),
        )
