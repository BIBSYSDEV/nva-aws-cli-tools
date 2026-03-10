from datetime import datetime

import click

from commands.services.scientific_index_api import ScientificIndexService
from commands.utils import AppContext


@click.group(name="reports")
@click.pass_obj
def reports(ctx: AppContext):
    pass


@reports.command(name="author-shares")
@click.option("--institution-id", "-i", required=True, help="Cristin institution ID (e.g. 185.90.0.0).")
@click.option("--year", default=lambda: datetime.now().year, show_default="current year", type=int)
@click.option("--output", default=None, help="Output filename (defaults to author_shares_<profile>_<id>_<year>_<timestamp>.xlsx)")
@click.pass_obj
def author_shares(ctx: AppContext, institution_id: str, year: int, output: str | None):
    service = ScientificIndexService(ctx.profile)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = output or f"author_shares_{ctx.profile}_{institution_id}_{year}_{timestamp}.xlsx"
    click.echo(f"Fetching report for {institution_id} ({year})...")
    data = service.get_institution_report(institution_id, year)
    with open(filename, "wb") as file:
        file.write(data)
    click.echo(f"Saved to {filename}")


@reports.command(name="author-shares-all")
@click.option("--year", default=lambda: datetime.now().year, show_default="current year", type=int)
@click.option("--output", default=None, help="Output filename (defaults to author_shares_<profile>_all_<year>_<timestamp>.xlsx)")
@click.pass_obj
def author_shares_all(ctx: AppContext, year: int, output: str | None):
    service = ScientificIndexService(ctx.profile)
    try:
        merged = service.get_all_institution_reports(ctx.profile, year)
    except ValueError as error:
        click.echo(str(error), err=True)
        raise click.Abort()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = output or f"author_shares_{ctx.profile}_all_{year}_{timestamp}.xlsx"
    merged.write_excel(filename, autofit=True)
    click.echo(f"Merged {len(merged)} rows into {filename}")
