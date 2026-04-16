import io
import logging
import warnings
from datetime import datetime

import click
import polars as pl

from commands.services.scientific_index_api import ScientificIndexService
from commands.utils import AppContext


@click.group(name="reports")
@click.pass_obj
def reports(ctx: AppContext):
    pass


@reports.command(name="author-shares")
@click.option("--year", default=lambda: datetime.now().year, show_default="current year", type=int)
@click.option("--output", default=None, help="Output filename (defaults to author_shares_<profile>_<year>_<timestamp>.xlsx)")
@click.pass_obj
def author_shares(ctx: AppContext, year: int, output: str | None):
    service = ScientificIndexService(ctx.profile)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = output or f"author_shares_{ctx.profile}_{year}_{timestamp}.xlsx"
    click.echo(f"Fetching author shares report for {year} (may take a few minutes)...")
    data = service.get_all_institutions_report(year)
    if not logging.getLogger().isEnabledFor(logging.DEBUG):
        warnings.filterwarnings("ignore", message="Ignoring URL", category=UserWarning)
    pl.read_excel(io.BytesIO(data)).write_excel(filename, autofit=True, table_style="Table Style Medium 9")
    click.echo(f"Saved to {filename}")
