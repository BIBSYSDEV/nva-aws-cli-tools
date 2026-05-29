import functools
import logging
from typing import Optional

import click
import requests
from rich.console import Console
from rich.table import Table

from commands.services.channels_api import (
    KIND_JOURNAL,
    KIND_PUBLISHER,
    KIND_SERIAL,
    KIND_SERIES,
    ChannelNotFoundError,
    ChannelsApiService,
)
from commands.utils import AppContext

logger = logging.getLogger(__name__)

KIND_CHOICES = ["serial", "journal", "series", "publisher"]


def _handle_api_errors(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ChannelNotFoundError as exc:
            raise click.ClickException(str(exc))
        except requests.HTTPError as exc:
            raise click.ClickException(_format_http_error(exc))

    return wrapper


@click.group()
@click.pass_obj
def channels(ctx: AppContext):
    """Manage publication channels (journals, series, publishers).

    Type is auto-detected where possible so you rarely need --kind.
    """
    pass


@channels.command()
@click.argument("query")
@click.option(
    "--kind",
    type=click.Choice(KIND_CHOICES),
    default=None,
    help="Restrict to a single channel kind. Default: search all kinds.",
)
@click.option("--year", type=int, default=None, help="Limit results to a given year")
@click.option("--offset", type=int, default=0)
@click.option("--size", type=int, default=10)
@click.pass_obj
@_handle_api_errors
def search(
    ctx: AppContext,
    query: str,
    kind: Optional[str],
    year: Optional[int],
    offset: int,
    size: int,
) -> None:
    """Search channels across journals/series/publishers."""
    service = ChannelsApiService(ctx.profile)
    rows = _collect_search_rows(service, query, kind, year, offset, size)
    _render_table(rows, query)


@channels.command()
@click.argument("identifier")
@click.option("--year", type=int, default=None, help="Fetch data for a given year")
@click.option(
    "--kind",
    type=click.Choice(KIND_CHOICES),
    default=None,
    help="Force lookup of a specific kind. Default: auto-detect.",
)
@click.pass_obj
@_handle_api_errors
def get(
    ctx: AppContext, identifier: str, year: Optional[int], kind: Optional[str]
) -> None:
    """Fetch a single channel by identifier (auto-detects type)."""
    service = ChannelsApiService(ctx.profile)
    if kind is None:
        channel = service.fetch_auto(identifier, year)
    else:
        channel = service.fetch(_resolve_kind(kind), identifier, year)
        channel.setdefault("_resolvedKind", _resolve_kind(kind))
    _print_channel(channel)


@channels.command()
@click.option("--name", required=True, help="Channel name")
@click.option(
    "--kind",
    type=click.Choice(["publisher", "journal", "series", "serial"]),
    default=None,
    help="Explicit kind. Default: inferred from other flags.",
)
@click.option("--isbn-prefix", default=None, help="Publisher only")
@click.option("--print-issn", default=None, help="Journal/series only")
@click.option("--online-issn", default=None, help="Journal/series only")
@click.option("--homepage", default=None, help="Channel homepage URL")
@click.pass_obj
@_handle_api_errors
def create(
    ctx: AppContext,
    name: str,
    kind: Optional[str],
    isbn_prefix: Optional[str],
    print_issn: Optional[str],
    online_issn: Optional[str],
    homepage: Optional[str],
) -> None:
    """Create a new channel. Picks publisher vs serial-publication from flags."""
    resolved_kind = _infer_create_kind(kind, isbn_prefix, print_issn, online_issn)
    service = ChannelsApiService(ctx.profile)

    if resolved_kind == KIND_PUBLISHER:
        if print_issn or online_issn:
            raise click.UsageError(
                "ISSN flags are not valid for publisher; remove or set --kind"
            )
        result = service.create_publisher(name, isbn_prefix, homepage)
    elif resolved_kind == KIND_JOURNAL:
        _reject_isbn(isbn_prefix)
        result = service.create_journal(name, print_issn, online_issn, homepage)
    elif resolved_kind == KIND_SERIES:
        _reject_isbn(isbn_prefix)
        result = service.create_series(name, print_issn, online_issn, homepage)
    else:
        _reject_isbn(isbn_prefix)
        result = service.create_serial_publication(
            name, "Journal", print_issn, online_issn, homepage
        )

    click.echo(f"CREATED {resolved_kind}: {name}")
    _print_channel(result)


@channels.command()
@click.argument("identifier")
@click.option("--name", default=None, help="New name")
@click.option("--isbn", default=None, help="Publisher only")
@click.option("--print-issn", default=None, help="Serial publication only")
@click.option("--online-issn", default=None, help="Serial publication only")
@click.pass_obj
@_handle_api_errors
def update(
    ctx: AppContext,
    identifier: str,
    name: Optional[str],
    isbn: Optional[str],
    print_issn: Optional[str],
    online_issn: Optional[str],
) -> None:
    """Update an existing channel. Type is detected from the channel itself."""
    if name is None and isbn is None and print_issn is None and online_issn is None:
        raise click.UsageError("Specify at least one field to update.")

    service = ChannelsApiService(ctx.profile)
    existing = service.fetch_auto(identifier)
    resolved_kind = existing.get("_resolvedKind")

    if resolved_kind == KIND_PUBLISHER:
        if print_issn or online_issn:
            raise click.UsageError("ISSN flags are not valid for a publisher channel")
        service.update_publisher(identifier, name=name, isbn=isbn)
    else:
        if isbn:
            raise click.UsageError("--isbn is only valid for publisher channels")
        service.update_serial_publication(
            identifier, name=name, print_issn=print_issn, online_issn=online_issn
        )
    click.echo(f"UPDATED {resolved_kind} {identifier}")


@channels.command()
@click.argument("identifier")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt")
@click.pass_obj
@_handle_api_errors
def delete(ctx: AppContext, identifier: str, yes: bool) -> None:
    """Delete a channel by identifier."""
    service = ChannelsApiService(ctx.profile)
    existing = service.fetch_auto(identifier)

    name = existing.get("name", "?")
    resolved_kind = existing.get("_resolvedKind", "?")
    if not yes:
        click.confirm(f"Delete {resolved_kind} '{name}' ({identifier})?", abort=True)

    service.delete_channel(identifier)
    click.echo(f"DELETED {resolved_kind} {identifier}")


def _resolve_kind(kind: str) -> str:
    return KIND_SERIAL if kind == "serial" else kind


def _format_http_error(exc: requests.HTTPError) -> str:
    response = exc.response
    if response is None:
        return f"API error: {exc}"
    snippet = response.text[:500].strip() if response.text else ""
    base = f"API error {response.status_code} from {response.url}"
    return f"{base}\n  {snippet}" if snippet else base


def _infer_create_kind(
    kind: Optional[str],
    isbn_prefix: Optional[str],
    print_issn: Optional[str],
    online_issn: Optional[str],
) -> str:
    if kind:
        return _resolve_kind(kind)
    if isbn_prefix:
        return KIND_PUBLISHER
    if print_issn or online_issn:
        return KIND_SERIAL
    raise click.UsageError(
        "Cannot infer channel kind. Pass --kind or one of "
        "--isbn-prefix / --print-issn / --online-issn."
    )


def _reject_isbn(isbn_prefix: Optional[str]) -> None:
    if isbn_prefix:
        raise click.UsageError("--isbn-prefix is only valid for publisher channels")


def _collect_search_rows(
    service: ChannelsApiService,
    query: str,
    kind: Optional[str],
    year: Optional[int],
    offset: int,
    size: int,
) -> list:
    rows: list = []
    if kind in (None, "serial", "journal", "series"):
        serial_kind = _resolve_kind(kind) if kind else KIND_SERIAL
        rows.extend(
            _rows_from_hits(service.search(serial_kind, query, year, offset, size))
        )
    if kind in (None, "publisher"):
        rows.extend(
            _rows_from_hits(service.search(KIND_PUBLISHER, query, year, offset, size))
        )
    return rows


def _rows_from_hits(payload: dict) -> list:
    return [_row_from_hit(hit) for hit in payload.get("hits", [])]


def _row_from_hit(hit: dict) -> dict:
    return {
        "type": hit.get("type", "?"),
        "identifier": _identifier_from_id(hit.get("id", "")),
        "name": hit.get("name", ""),
        "issn_or_isbn": (
            hit.get("printIssn") or hit.get("onlineIssn") or hit.get("isbnPrefix") or ""
        ),
        "year": hit.get("year") or "",
    }


def _identifier_from_id(channel_id: str) -> str:
    if not channel_id:
        return ""
    stripped = channel_id.rstrip("/")
    parts = stripped.split("/")
    if len(parts) >= 2 and parts[-1].isdigit() and len(parts[-1]) == 4:
        return parts[-2]
    return parts[-1]


def _render_table(rows: list, query: str) -> None:
    console = Console()
    if not rows:
        console.print(f"[yellow]No hits for {query!r}[/yellow]")
        return
    table = Table(
        show_header=True,
        header_style="bold cyan",
        title=f"[bold magenta]Channels matching {query!r} ({len(rows)} hits)[/bold magenta]",
    )
    table.add_column("Type")
    table.add_column("Identifier")
    table.add_column("Name")
    table.add_column("ISSN/ISBN")
    table.add_column("Year")
    for row in rows:
        table.add_row(
            row["type"],
            row["identifier"],
            row["name"],
            row["issn_or_isbn"],
            str(row["year"]),
        )
    console.print(table)


def _print_channel(channel: dict) -> None:
    console = Console()
    resolved = channel.get("_resolvedKind")
    if resolved:
        console.print(f"[bold]Resolved kind:[/bold] {resolved}")
    keys = [
        "id",
        "type",
        "name",
        "printIssn",
        "onlineIssn",
        "isbnPrefix",
        "homepage",
        "scientificValue",
        "year",
        "sameAs",
    ]
    for key in keys:
        if key in channel and channel[key] not in (None, ""):
            console.print(f"  {key}: {channel[key]}")
