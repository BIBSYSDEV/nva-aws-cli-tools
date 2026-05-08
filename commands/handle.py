import csv
import click
import logging
import os
from datetime import datetime
from commands.utils import AppContext
from commands.services.handle_api import HandleApiService
from commands.services.publication_api import extract_publication_identifier
from commands.services.search_api import SearchApiService

logger = logging.getLogger(__name__)

APPLICATION_DOMAIN_PARAMETER = "/NVA/ApplicationDomain"
REGISTRATION_PATH = "registration"
DONE_CSV = "handle-done.csv"
CSV_FIELDS = ["handle", "target_url", "timestamp", "status"]


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
    Results are appended to handle-done.csv to allow resuming.

    Accepts one or more handles, or pipe from stdin:
      handle redirect-to-nva 11250/2497055 11250/2496565
      tail -n +2 nve-handles.csv | cut -d',' -f2 | xargs handle redirect-to-nva
    """
    handle_service = HandleApiService(ctx.profile)
    search_service = SearchApiService(ctx.profile)
    app_domain = handle_service._get_system_parameter(APPLICATION_DOMAIN_PARAMETER)
    registration_base_url = f"https://{app_domain}/{REGISTRATION_PATH}"

    if dry_run:
        click.echo("DRY RUN - no handles will be updated")

    done = _load_done(DONE_CSV)
    for handle_value in handles:
        if handle_value in done:
            click.echo(f"SKIP {handle_value}: already processed")
            continue
        _process_handle(
            handle_service,
            search_service,
            handle_value,
            registration_base_url,
            dry_run,
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


def _load_done(csv_path: str) -> set:
    if not os.path.exists(csv_path):
        return set()
    with open(csv_path, newline="") as f:
        return {row["handle"] for row in csv.DictReader(f)}


def _append_result(handle_value: str, target_url: str, status: str) -> None:
    file_exists = os.path.exists(DONE_CSV)
    with open(DONE_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "handle": handle_value,
            "target_url": target_url,
            "timestamp": datetime.now().isoformat(),
            "status": status,
        })


def _process_handle(
    handle_service: HandleApiService,
    search_service: SearchApiService,
    handle_value: str,
    registration_base_url: str,
    dry_run: bool,
) -> None:
    hits = search_service.find_by_handle(handle_value)
    if len(hits) != 1:
        click.echo(f"SKIP {handle_value}: found {len(hits)} hits")
        _append_result(handle_value, "", "skipped")
        return

    identifier = extract_publication_identifier(hits[0].get("id", ""))
    nva_url = f"{registration_base_url}/{identifier}"

    if dry_run:
        click.echo(f"DRY-RUN {handle_value} → {nva_url}")
        return

    try:
        result = handle_service.set_handle(handle_value, nva_url)
        _append_result(handle_value, nva_url, "ok")
        click.echo(f"UPDATED {handle_value} → {nva_url} ({result})")
    except Exception as exc:
        _append_result(handle_value, nva_url, "failed")
        click.echo(f"FAILED {handle_value}: {exc}")
