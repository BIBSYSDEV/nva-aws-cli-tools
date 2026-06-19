import glob
import io
import json

import boto3
import polars as pl
import responses
from click.testing import CliRunner
from moto import mock_aws

from commands.reports import reports
from commands.utils import AppContext

API_DOMAIN = "api.example.org"
COGNITO_URL = "https://cognito.example.org/oauth2/token"
PRESIGNED_URL = "https://s3.example.org/report.xlsx"
A_YEAR = 2024
A_PROFILE = "testprofile"
AN_INSTITUTION = "20754.0.0.0"
ALL_INSTITUTIONS_URL = (
    f"https://{API_DOMAIN}/scientific-index/reports/{A_YEAR}/institutions"
)
INSTITUTION_URL = f"{ALL_INSTITUTIONS_URL}/{AN_INSTITUTION}"


def _ctx() -> AppContext:
    return AppContext(
        log_level=0,
        profile=A_PROFILE,
        session=boto3.Session(region_name="eu-west-1"),
    )


def _seed_aws() -> None:
    ssm = boto3.client("ssm", region_name="eu-west-1")
    ssm.put_parameter(Name="/NVA/ApiDomain", Value=API_DOMAIN, Type="String")
    ssm.put_parameter(
        Name="/NVA/CognitoUri", Value="https://cognito.example.org", Type="String"
    )
    boto3.client("secretsmanager", region_name="eu-west-1").create_secret(
        Name="BackendCognitoClientCredentials",
        SecretString=json.dumps(
            {"backendClientId": "id", "backendClientSecret": "secret"}
        ),
    )


def _add_cognito() -> None:
    responses.add(
        responses.POST, COGNITO_URL, json={"access_token": "token", "expires_in": 3600}
    )


def _an_xlsx_report() -> bytes:
    buffer = io.BytesIO()
    pl.DataFrame({"institution": ["NTNU"], "shares": [1.0]}).write_excel(buffer)
    return buffer.getvalue()


def _add_report(report_url: str) -> None:
    responses.add(responses.GET, report_url, json={"uri": PRESIGNED_URL})
    responses.add(responses.GET, PRESIGNED_URL, body=_an_xlsx_report(), status=200)


@mock_aws
@responses.activate
def test_author_shares_without_institution_targets_all_institutions():
    _seed_aws()
    _add_cognito()
    _add_report(ALL_INSTITUTIONS_URL)

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            reports, ["author-shares", "--year", str(A_YEAR)], obj=_ctx()
        )

        assert result.exit_code == 0, result.output
        files = glob.glob("*.xlsx")
        assert len(files) == 1
        assert files[0].startswith(f"author_shares_{A_PROFILE}_{A_YEAR}_")
        assert AN_INSTITUTION not in files[0]
    assert any(c.request.url == ALL_INSTITUTIONS_URL for c in responses.calls)


@mock_aws
@responses.activate
def test_author_shares_with_institution_targets_institution_and_names_file():
    _seed_aws()
    _add_cognito()
    _add_report(INSTITUTION_URL)

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            reports,
            ["author-shares", "--year", str(A_YEAR), "--institution", AN_INSTITUTION],
            obj=_ctx(),
        )

        assert result.exit_code == 0, result.output
        files = glob.glob("*.xlsx")
        assert len(files) == 1
        assert files[0].startswith(
            f"author_shares_{A_PROFILE}_{A_YEAR}_{AN_INSTITUTION}_"
        )
    assert any(c.request.url == INSTITUTION_URL for c in responses.calls)
    assert all(c.request.url != ALL_INSTITUTIONS_URL for c in responses.calls)


@mock_aws
@responses.activate
def test_author_shares_respects_explicit_output_filename():
    _seed_aws()
    _add_cognito()
    _add_report(ALL_INSTITUTIONS_URL)

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            reports,
            ["author-shares", "--year", str(A_YEAR), "--output", "custom.xlsx"],
            obj=_ctx(),
        )

        assert result.exit_code == 0, result.output
        assert glob.glob("*.xlsx") == ["custom.xlsx"]
