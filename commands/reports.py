import io
import logging
import warnings
from datetime import datetime

import click
import polars as pl

from commands.services.api_client import ApiClient
from commands.services.scientific_index_api import (
    get_all_institutions_report,
    get_all_institutions_report_control,
)
from commands.utils import AppContext


@click.group(name="reports")
@click.pass_obj
def reports(ctx: AppContext):
    pass


@reports.command(name="author-shares")
@click.option(
    "--year", default=lambda: datetime.now().year, show_default="current year", type=int
)
@click.option(
    "--output",
    default=None,
    help="Output filename (defaults to author_shares_<profile>_<year>_<timestamp>.xlsx)",
)
@click.pass_obj
def author_shares(ctx: AppContext, year: int, output: str | None):
    _run_report(
        ctx,
        year,
        output,
        report_name="author_shares",
        fetch=get_all_institutions_report,
    )


@reports.command(name="author-shares-control")
@click.option(
    "--year", default=lambda: datetime.now().year, show_default="current year", type=int
)
@click.option(
    "--output",
    default=None,
    help="Output filename (defaults to author_shares_control_<profile>_<year>_<timestamp>.xlsx)",
)
@click.pass_obj
def author_shares_control(ctx: AppContext, year: int, output: str | None):
    _run_report(
        ctx,
        year,
        output,
        report_name="author_shares_control",
        fetch=get_all_institutions_report_control,
    )


def _run_report(
    ctx: AppContext,
    year: int,
    output: str | None,
    report_name: str,
    fetch,
):
    client = ApiClient(session=ctx.session)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = output or f"{report_name}_{ctx.profile}_{year}_{timestamp}.xlsx"
    click.echo(f"Fetching {report_name} report for {year} (may take a few minutes)...")
    data = fetch(client, year)
    if not logging.getLogger().isEnabledFor(logging.DEBUG):
        warnings.filterwarnings("ignore", message="Ignoring URL", category=UserWarning)
    pl.read_excel(io.BytesIO(data)).write_excel(
        filename, autofit=True, table_style="Table Style Medium 9"
    )
    click.echo(f"Saved to {filename}")
