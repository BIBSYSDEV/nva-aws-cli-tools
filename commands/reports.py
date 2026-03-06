import io
import sys
from datetime import datetime

import click
import polars as pl
from tqdm import tqdm

from commands.services.customers_api import get_all_customers
from commands.services.scientific_index_api import ScientificIndexService
from commands.utils import AppContext


@click.group(name="reports")
@click.pass_obj
def reports(ctx: AppContext):
    pass


@reports.command(name="author-shares")
@click.option("--institution-id", default=None, help="Cristin institution ID (e.g. 185.90.0.0). Omit to export all NVI institutions.")
@click.option("--year", default=lambda: datetime.now().year, show_default="current year", type=int)
@click.option("--output", default=None, help="Output filename (defaults to author_shares_<id>_<year>.xlsx or author_shares_all_<year>.xlsx)")
@click.pass_obj
def author_shares(ctx: AppContext, institution_id: str | None, year: int, output: str | None):
    service = ScientificIndexService(ctx.profile)

    if institution_id:
        _export_single(service, institution_id, year, output)
    else:
        _export_all(ctx.profile, service, year, output)


def _export_single(service: ScientificIndexService, institution_id: str, year: int, output: str | None) -> None:
    filename = output or f"author_shares_{institution_id}_{year}.xlsx"
    click.echo(f"Fetching report for {institution_id} ({year})...")
    data = service.get_institution_report(institution_id, year)
    with open(filename, "wb") as file:
        file.write(data)
    click.echo(f"Saved to {filename}")


def _export_all(profile: str, service: ScientificIndexService, year: int, output: str | None) -> None:
    nvi_customers = [
        customer
        for customer in get_all_customers(profile)
        if customer.nvi_institution and customer.cristin_id
    ]

    if not nvi_customers:
        click.echo("No NVI institutions found.", err=True)
        sys.exit(1)

    click.echo(f"Found {len(nvi_customers)} NVI institutions. Fetching reports for {year}...")

    frames: list[pl.DataFrame] = []
    errors: list[str] = []

    for customer in tqdm(nvi_customers, desc="Fetching reports"):
        cristin_short_id = customer.cristin_id.rsplit("/", 1)[-1]
        try:
            data = service.get_institution_report(cristin_short_id, year)
            df = pl.read_excel(io.BytesIO(data), raise_if_empty=False)
            if len(df) > 0:
                frames.append(df)
        except Exception as error:
            errors.append(f"{customer.name} ({cristin_short_id}): {error}")

    if errors:
        click.echo(f"\nFailed to fetch {len(errors)} reports:", err=True)
        for error in errors:
            click.echo(f"  {error}", err=True)

    if not frames:
        click.echo("No reports fetched successfully.", err=True)
        sys.exit(1)

    filename = output or f"author_shares_all_{year}.xlsx"
    merged = pl.concat(frames, how="diagonal")
    merged.write_excel(filename, autofit=True)
    click.echo(f"Merged {len(frames)} reports ({len(merged)} rows) into {filename}")
