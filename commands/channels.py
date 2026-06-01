import functools

import click
import requests
from rich.console import Console
from rich.table import Table

from commands.services.api_client import ApiClient
from commands.services.channels_api import (
    KIND_JOURNAL,
    KIND_PUBLISHER,
    KIND_SERIAL,
    KIND_SERIES,
    SERIAL_TYPE_JOURNAL,
    VALID_KINDS,
    ChannelNotFoundError,
    ChannelsApiService,
)
from commands.utils import AppContext

KIND_ALIAS_SERIAL = "serial"
KIND_CHOICES = [KIND_ALIAS_SERIAL, KIND_JOURNAL, KIND_SERIES, KIND_PUBLISHER]
SERIAL_LIKE_CHOICES = (None, KIND_ALIAS_SERIAL, KIND_JOURNAL, KIND_SERIES)


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
@click.option(
    "--offset", type=int, default=0, help="Applied per kind when --kind is omitted"
)
@click.option(
    "--size",
    type=int,
    default=10,
    help="Max hits per kind (when --kind is omitted, both serial and publisher are queried, so up to 2x this many rows are returned)",
)
@click.pass_obj
@_handle_api_errors
def search(
    ctx: AppContext,
    query: str,
    kind: str | None,
    year: int | None,
    offset: int,
    size: int,
) -> None:
    """Search channels across journals/series/publishers."""
    service = ChannelsApiService(ApiClient(session=ctx.session))
    rows, total_hits = _collect_search_rows(service, query, kind, year, offset, size)
    _render_table(rows, query, total_hits)


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
def get(ctx: AppContext, identifier: str, year: int | None, kind: str | None) -> None:
    """Fetch a single channel by identifier (auto-detects type)."""
    service = ChannelsApiService(ApiClient(session=ctx.session))
    if kind is None:
        channel, resolved_kind = service.fetch_auto(identifier, year)
    else:
        resolved_kind = _resolve_kind(kind)
        channel = service.fetch(resolved_kind, identifier, year)
    _print_channel(channel, resolved_kind)


@channels.command()
@click.option("--name", required=True, help="Channel name")
@click.option(
    "--kind",
    type=click.Choice(KIND_CHOICES),
    default=None,
    help="Explicit kind. Default: inferred from other flags.",
)
@click.option("--isbn", default=None, help="Publisher only (ISBN prefix)")
@click.option("--print-issn", default=None, help="Journal/series only")
@click.option("--online-issn", default=None, help="Journal/series only")
@click.pass_obj
@_handle_api_errors
def create(
    ctx: AppContext,
    name: str,
    kind: str | None,
    isbn: str | None,
    print_issn: str | None,
    online_issn: str | None,
) -> None:
    """Create a new channel. Picks publisher vs serial-publication from flags."""
    resolved_kind = _infer_create_kind(kind, isbn, print_issn, online_issn)
    service = ChannelsApiService(ApiClient(session=ctx.session))

    if resolved_kind == KIND_PUBLISHER:
        if print_issn or online_issn:
            raise click.UsageError(
                "ISSN flags are not valid for publisher; remove or set --kind"
            )
        result = service.create_publisher(name, isbn)
    elif resolved_kind == KIND_JOURNAL:
        _reject_isbn(isbn)
        result = service.create_journal(name, print_issn, online_issn)
    elif resolved_kind == KIND_SERIES:
        _reject_isbn(isbn)
        result = service.create_series(name, print_issn, online_issn)
    else:
        _reject_isbn(isbn)
        result = service.create_serial_publication(
            name, SERIAL_TYPE_JOURNAL, print_issn, online_issn
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
    name: str | None,
    isbn: str | None,
    print_issn: str | None,
    online_issn: str | None,
) -> None:
    """Update an existing channel. Type is detected from the channel itself."""
    if name is None and isbn is None and print_issn is None and online_issn is None:
        raise click.UsageError("Specify at least one field to update.")

    service = ChannelsApiService(ApiClient(session=ctx.session))
    _, resolved_kind = service.fetch_auto(identifier)

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
    service = ChannelsApiService(ApiClient(session=ctx.session))
    existing, resolved_kind = service.fetch_auto(identifier)

    name = existing.get("name", "?")
    if not yes:
        click.confirm(f"Delete {resolved_kind} '{name}' ({identifier})?", abort=True)

    service.delete_channel(identifier)
    click.echo(f"DELETED {resolved_kind} {identifier}")


def _resolve_kind(kind: str) -> str:
    return KIND_SERIAL if kind == KIND_ALIAS_SERIAL else kind


def _format_http_error(exc: requests.HTTPError) -> str:
    response = exc.response
    if response is None:
        return f"API error: {exc}"
    snippet = response.text[:500].strip() if response.text else ""
    base = f"API error {response.status_code} from {response.url}"
    return f"{base}\n  {snippet}" if snippet else base


def _infer_create_kind(
    kind: str | None,
    isbn: str | None,
    print_issn: str | None,
    online_issn: str | None,
) -> str:
    if kind:
        return _resolve_kind(kind)
    if isbn:
        return KIND_PUBLISHER
    if print_issn or online_issn:
        return KIND_SERIAL
    raise click.UsageError(
        "Cannot infer channel kind. Pass --kind or one of "
        "--isbn / --print-issn / --online-issn."
    )


def _reject_isbn(isbn: str | None) -> None:
    if isbn:
        raise click.UsageError("--isbn is only valid for publisher channels")


def _collect_search_rows(
    service: ChannelsApiService,
    query: str,
    kind: str | None,
    year: int | None,
    offset: int,
    size: int,
) -> tuple[list, int]:
    rows: list = []
    total_hits = 0
    if kind in SERIAL_LIKE_CHOICES:
        serial_kind = _resolve_kind(kind) if kind else KIND_SERIAL
        payload = service.search(serial_kind, query, year, offset, size)
        rows.extend(_rows_from_hits(payload))
        total_hits += _total_hits(payload)
    if kind in (None, KIND_PUBLISHER):
        payload = service.search(KIND_PUBLISHER, query, year, offset, size)
        rows.extend(_rows_from_hits(payload))
        total_hits += _total_hits(payload)
    return rows, total_hits


def _total_hits(payload: dict) -> int:
    value = payload.get("totalHits")
    if isinstance(value, int):
        return value
    return len(payload.get("hits", []))


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
    parts = [segment for segment in channel_id.split("/") if segment]
    for index, segment in enumerate(parts):
        if segment in VALID_KINDS and index + 1 < len(parts):
            return parts[index + 1]
    return parts[-1] if parts else ""


def _render_table(rows: list, query: str, total_hits: int) -> None:
    console = Console()
    if not rows:
        console.print(f"[yellow]No hits for {query!r}[/yellow]")
        return
    title = (
        f"[bold magenta]Channels matching {query!r} "
        f"(showing {len(rows)} of {total_hits})[/bold magenta]"
    )
    table = Table(
        show_header=True,
        header_style="bold cyan",
        title=title,
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


def _print_channel(channel: dict, resolved_kind: str | None = None) -> None:
    console = Console()
    if resolved_kind:
        console.print(f"[bold]Resolved kind:[/bold] {resolved_kind}")
    keys = [
        "id",
        "location",
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
