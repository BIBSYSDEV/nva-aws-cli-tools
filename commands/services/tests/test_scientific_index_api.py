import json

import boto3
import pytest
import responses
from moto import mock_aws

from commands.services.api_client import ApiClient
from commands.services.scientific_index_api import (
    XLSX_AUTHOR_SHARES_ACCEPT,
    XLSX_AUTHOR_SHARES_CONTROL_ACCEPT,
    get_all_institutions_report,
    get_all_institutions_report_control,
)

API_DOMAIN = "api.example.org"
COGNITO_URL = "https://cognito.example.org/oauth2/token"
PRESIGNED_URL = "https://s3.example.org/report.xlsx"
A_YEAR = 2024
AN_INSTITUTION = "20754.0.0.0"
REPORT_BYTES = b"xlsx-content"
ALL_INSTITUTIONS_URL = (
    f"https://{API_DOMAIN}/scientific-index/reports/{A_YEAR}/institutions"
)
INSTITUTION_URL = f"{ALL_INSTITUTIONS_URL}/{AN_INSTITUTION}"


def _client() -> ApiClient:
    return ApiClient(session=boto3.Session(region_name="eu-west-1"))


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


def _add_report_redirect(url: str) -> None:
    responses.add(responses.GET, url, json={"uri": PRESIGNED_URL})


def _add_presigned_xlsx() -> None:
    responses.add(responses.GET, PRESIGNED_URL, body=REPORT_BYTES, status=200)


@mock_aws
@responses.activate
def test_all_institutions_report_uses_all_institutions_path():
    _seed_aws()
    _add_cognito()
    _add_report_redirect(ALL_INSTITUTIONS_URL)
    _add_presigned_xlsx()

    data = get_all_institutions_report(_client(), A_YEAR)

    assert data == REPORT_BYTES
    report_call = next(
        c for c in responses.calls if c.request.url == ALL_INSTITUTIONS_URL
    )
    assert report_call.request.headers["Accept"] == XLSX_AUTHOR_SHARES_ACCEPT


@mock_aws
@responses.activate
def test_report_with_institution_uses_institution_path():
    _seed_aws()
    _add_cognito()
    _add_report_redirect(INSTITUTION_URL)
    _add_presigned_xlsx()

    data = get_all_institutions_report(_client(), A_YEAR, institution=AN_INSTITUTION)

    assert data == REPORT_BYTES
    assert any(c.request.url == INSTITUTION_URL for c in responses.calls)
    assert all(c.request.url != ALL_INSTITUTIONS_URL for c in responses.calls)


@mock_aws
@responses.activate
def test_control_report_uses_control_accept_header():
    _seed_aws()
    _add_cognito()
    _add_report_redirect(ALL_INSTITUTIONS_URL)
    _add_presigned_xlsx()

    get_all_institutions_report_control(_client(), A_YEAR)

    report_call = next(
        c for c in responses.calls if c.request.url == ALL_INSTITUTIONS_URL
    )
    assert report_call.request.headers["Accept"] == XLSX_AUTHOR_SHARES_CONTROL_ACCEPT


@mock_aws
@responses.activate
def test_control_report_with_institution_uses_institution_path_and_control_accept():
    _seed_aws()
    _add_cognito()
    _add_report_redirect(INSTITUTION_URL)
    _add_presigned_xlsx()

    get_all_institutions_report_control(_client(), A_YEAR, institution=AN_INSTITUTION)

    report_call = next(c for c in responses.calls if c.request.url == INSTITUTION_URL)
    assert report_call.request.headers["Accept"] == XLSX_AUTHOR_SHARES_CONTROL_ACCEPT


@mock_aws
@responses.activate
def test_report_polls_until_presigned_url_ready(monkeypatch):
    monkeypatch.setattr(
        "commands.services.scientific_index_api.time.sleep", lambda _: None
    )
    _seed_aws()
    _add_cognito()
    _add_report_redirect(ALL_INSTITUTIONS_URL)
    responses.add(responses.GET, PRESIGNED_URL, status=404)
    responses.add(responses.GET, PRESIGNED_URL, body=REPORT_BYTES, status=200)

    data = get_all_institutions_report(_client(), A_YEAR)

    assert data == REPORT_BYTES
    presigned_calls = [c for c in responses.calls if c.request.url == PRESIGNED_URL]
    assert len(presigned_calls) == 2


@mock_aws
@responses.activate
def test_report_raises_timeout_when_never_ready():
    _seed_aws()
    _add_cognito()
    _add_report_redirect(ALL_INSTITUTIONS_URL)

    with pytest.raises(TimeoutError):
        get_all_institutions_report(_client(), A_YEAR, timeout_minutes=0)


@mock_aws
@responses.activate
def test_report_raises_on_server_error_from_presigned_url():
    _seed_aws()
    _add_cognito()
    _add_report_redirect(ALL_INSTITUTIONS_URL)
    responses.add(responses.GET, PRESIGNED_URL, status=500)

    with pytest.raises(Exception):
        get_all_institutions_report(_client(), A_YEAR)
