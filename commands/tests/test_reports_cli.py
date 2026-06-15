import io
import os
from unittest.mock import patch

import boto3
import polars as pl
from click.testing import CliRunner

from commands.reports import reports
from commands.utils import AppContext

A_PROFILE = "test"
A_YEAR = 2024


def _ctx() -> AppContext:
    return AppContext(
        log_level=0,
        profile=A_PROFILE,
        session=boto3.Session(region_name="eu-west-1"),
    )


def _xlsx_bytes() -> bytes:
    buffer = io.BytesIO()
    pl.DataFrame({"institution": ["NTNU"], "shares": [1.0]}).write_excel(buffer)
    return buffer.getvalue()


def test_author_shares_calls_author_shares_fetch_and_names_file():
    runner = CliRunner()
    with runner.isolated_filesystem():
        with (
            patch(
                "commands.reports.get_all_institutions_report",
                return_value=_xlsx_bytes(),
            ) as fetch,
            patch(
                "commands.reports.get_all_institutions_report_control"
            ) as control_fetch,
        ):
            result = runner.invoke(
                reports, ["author-shares", "--year", str(A_YEAR)], obj=_ctx()
            )

        assert result.exit_code == 0, result.output
        fetch.assert_called_once()
        control_fetch.assert_not_called()
        written = os.listdir(".")
        assert len(written) == 1
        assert written[0].startswith(f"author_shares_{A_PROFILE}_{A_YEAR}_")
        assert written[0].endswith(".xlsx")


def test_author_shares_control_calls_control_fetch_and_names_file():
    runner = CliRunner()
    with runner.isolated_filesystem():
        with (
            patch(
                "commands.reports.get_all_institutions_report_control",
                return_value=_xlsx_bytes(),
            ) as control_fetch,
            patch("commands.reports.get_all_institutions_report") as fetch,
        ):
            result = runner.invoke(
                reports, ["author-shares-control", "--year", str(A_YEAR)], obj=_ctx()
            )

        assert result.exit_code == 0, result.output
        control_fetch.assert_called_once()
        fetch.assert_not_called()
        written = os.listdir(".")
        assert len(written) == 1
        assert written[0].startswith(f"author_shares_control_{A_PROFILE}_{A_YEAR}_")
        assert written[0].endswith(".xlsx")


def test_output_option_overrides_generated_filename():
    runner = CliRunner()
    with runner.isolated_filesystem():
        with patch(
            "commands.reports.get_all_institutions_report",
            return_value=_xlsx_bytes(),
        ):
            result = runner.invoke(
                reports,
                ["author-shares", "--year", str(A_YEAR), "--output", "custom.xlsx"],
                obj=_ctx(),
            )

        assert result.exit_code == 0, result.output
        assert os.path.exists("custom.xlsx")
