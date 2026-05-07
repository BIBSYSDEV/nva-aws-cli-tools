import click
import logging
import sqlite3
from datetime import datetime
from commands.utils import AppContext
from commands.services.handle_api import HandleApiService
from commands.services.publication_api import extract_publication_identifier
from commands.services.search_api import SearchApiService

logger = logging.getLogger(__name__)

APPLICATION_DOMAIN_PARAMETER = "/NVA/ApplicationDomain"
REGISTRATION_PATH = "registration"
DONE_DB_NAME = "handle-done.db"


@click.group()
@click.pass_obj
def handle(ctx: AppContext):
    pass


@handle.command()
@click.argument("handles", nargs=-1, required=True)
@click.option(
    "--dry-run", is_flag=True, default=False, help="Print updates without applying them"
)
@click.pass_obj
def redirect_to_nva(ctx: AppContext, handles: tuple, dry_run: bool) -> None:
    """Update handles to redirect to NVA registration pages.

    Searches each HANDLE in NVA and updates it to point to
    https://<ApplicationDomain>/registration/<identifier> if exactly one match is found.
    Processed handles are tracked in a local SQLite database to allow resuming.

    Accepts one or more handles, or pipe from stdin:
      handle redirect-to-nva 11250/2497055 11250/2496565
      cat handles.txt | xargs handle redirect-to-nva
    """
    handle_service = HandleApiService(ctx.profile)
    search_service = SearchApiService(ctx.profile)
    app_domain = handle_service._get_system_parameter(APPLICATION_DOMAIN_PARAMETER)
    registration_base_url = f"https://{app_domain}/{REGISTRATION_PATH}"

    if dry_run:
        click.echo("DRY RUN - no handles will be updated")

    with _open_done_db() as done_db:
        for handle_value in handles:
            if _is_done(done_db, handle_value):
                click.echo(f"SKIP {handle_value}: already processed")
                continue
            _process_handle(
                handle_service,
                search_service,
                handle_value,
                registration_base_url,
                dry_run,
                done_db,
            )


@handle.command()
@click.argument("handle_value")
@click.argument("target_url")
@click.pass_obj
def set_handle(ctx: AppContext, handle_value: str, target_url: str) -> None:
    """Update a single handle to point to TARGET_URL."""
    handle_service = HandleApiService(ctx.profile)
    result = handle_service.set_handle(handle_value, target_url)
    click.echo(f"UPDATED {handle_value} → {target_url} ({result})")


def _open_done_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DONE_DB_NAME)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS done (handle TEXT PRIMARY KEY, timestamp TEXT NOT NULL)"
    )
    conn.commit()
    return conn


def _is_done(conn: sqlite3.Connection, handle_value: str) -> bool:
    return (
        conn.execute("SELECT 1 FROM done WHERE handle = ?", (handle_value,)).fetchone()
        is not None
    )


def _mark_done(conn: sqlite3.Connection, handle_value: str) -> None:
    conn.execute(
        "INSERT INTO done (handle, timestamp) VALUES (?, ?)",
        (handle_value, datetime.now().isoformat()),
    )
    conn.commit()


def _process_handle(
    handle_service: HandleApiService,
    search_service: SearchApiService,
    handle_value: str,
    registration_base_url: str,
    dry_run: bool,
    done_db: sqlite3.Connection,
) -> None:
    hits = search_service.find_by_handle(handle_value)
    if len(hits) != 1:
        click.echo(f"SKIP {handle_value}: found {len(hits)} hits")
        return

    identifier = extract_publication_identifier(hits[0].get("id", ""))
    nva_url = f"{registration_base_url}/{identifier}"

    if dry_run:
        click.echo(f"DRY-RUN {handle_value} → {nva_url}")
        return

    result = handle_service.set_handle(handle_value, nva_url)
    _mark_done(done_db, handle_value)
    click.echo(f"UPDATED {handle_value} → {nva_url} ({result})")
