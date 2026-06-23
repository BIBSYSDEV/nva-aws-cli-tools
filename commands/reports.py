import io
import json
from datetime import datetime

import click
import polars as pl

from commands.services.api_client import ApiClient
from commands.services.report_compare import (
    diff_dataframes,
    diff_json,
    render_dataframe_diff,
    render_json_diff,
)
from commands.services.scientific_index_api import (
    ALL_INSTITUTIONS_REPORT_PATH,
    ALL_PERIODS_REPORT_PATH,
    fetch_report_json,
    get_all_institutions_report,
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
    "--institution",
    default=None,
    help="Institution identifier (e.g., 20754.0.0.0). Defaults to all institutions.",
)
@click.option(
    "--baseline",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Compare the freshly fetched xlsx report against this baseline file",
)
@click.option(
    "--save",
    default=None,
    help="Save the freshly fetched xlsx to this file (default name used when omitted and not comparing)",
)
@click.option(
    "--key",
    default="NVAID,PERSONLOPENR,INSTITUSJON,TITTEL,ETTERNAVN,FORNAVN",
    show_default=True,
    help="Comma-separated key columns used to match rows when comparing",
)
@click.pass_obj
def author_shares(
    ctx: AppContext,
    year: int,
    institution: str | None,
    baseline: str | None,
    save: str | None,
    key: str,
):
    client = ApiClient(session=ctx.session)
    scope = f"institution {institution}" if institution else "all institutions"
    click.echo(
        f"Fetching author shares report for {year} ({scope}) (may take a few minutes)..."
    )
    data = get_all_institutions_report(client, year, institution=institution)
    if save or not baseline:
        _save_xlsx(data, save or _default_xlsx_filename(ctx.profile, year, institution))
    if baseline:
        key_columns = [column.strip() for column in key.split(",") if column.strip()]
        render_dataframe_diff(
            diff_dataframes(
                pl.read_excel(baseline),
                pl.read_excel(io.BytesIO(data)),
                key_columns,
            )
        )


def _save_xlsx(data: bytes, filename: str) -> None:
    with open(filename, "wb") as output_file:
        output_file.write(data)
    click.echo(f"Saved to {filename}")


def _default_xlsx_filename(
    profile: str | None, year: int, institution: str | None
) -> str:
    institution_suffix = f"_{institution}" if institution else ""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"author_shares_{profile}_{year}{institution_suffix}_{timestamp}.xlsx"


@reports.command(name="nvi-all-periods")
@click.option(
    "--baseline",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Compare the freshly fetched report against this baseline file",
)
@click.option(
    "--save",
    default=None,
    help="Save the freshly fetched JSON to this file (default name used when omitted and not comparing)",
)
@click.pass_obj
def nvi_all_periods(ctx: AppContext, baseline: str | None, save: str | None):
    _fetch_json_report(ctx, ALL_PERIODS_REPORT_PATH, "all_periods", baseline, save)


@reports.command(name="nvi-institutions")
@click.option(
    "--year", default=lambda: datetime.now().year, show_default="current year", type=int
)
@click.option(
    "--baseline",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Compare the freshly fetched report against this baseline file",
)
@click.option(
    "--save",
    default=None,
    help="Save the freshly fetched JSON to this file (default name used when omitted and not comparing)",
)
@click.pass_obj
def nvi_institutions(
    ctx: AppContext, year: int, baseline: str | None, save: str | None
):
    path = ALL_INSTITUTIONS_REPORT_PATH.format(year=year)
    _fetch_json_report(ctx, path, str(year), baseline, save)


def _fetch_json_report(
    ctx: AppContext, path: str, scope: str, baseline: str | None, save: str | None
) -> None:
    client = ApiClient(session=ctx.session)
    click.echo(f"Fetching {path} from {client.api_domain} ...")
    current_data = fetch_report_json(client, path)
    if save or not baseline:
        _save_report(current_data, save or _default_filename(ctx.profile, scope))
    if baseline:
        render_json_diff(diff_json(_load_json(baseline), current_data))


def _save_report(data: dict, filename: str) -> None:
    with open(filename, "w", encoding="utf-8") as output_file:
        json.dump(data, output_file, indent=2, ensure_ascii=False)
    click.echo(f"Saved to {filename}")


def _default_filename(profile: str | None, scope: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"nvi_reports_{profile}_{scope}_{timestamp}.json"


def _load_json(path: str) -> dict:
    with open(path, encoding="utf-8") as json_file:
        return json.load(json_file)
