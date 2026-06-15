import json
from unittest.mock import patch

import boto3
import pytest
import requests
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
PRESIGNED_URL = "https://files.example.org/report.xlsx"
A_YEAR = 2024
REPORT_URL = f"https://{API_DOMAIN}/scientific-index/reports/{A_YEAR}/institutions"
XLSX_BYTES = b"PK\x03\x04 fake xlsx"


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


def _client() -> ApiClient:
    return ApiClient(session=boto3.Session(region_name="eu-west-1"))


def _report_accept_header() -> str:
    report_call = next(
        call for call in responses.calls if call.request.url.startswith(REPORT_URL)
    )
    return report_call.request.headers["Accept"]


@mock_aws
@responses.activate
def test_get_all_institutions_report_sends_author_shares_accept_header():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, REPORT_URL, json={"uri": PRESIGNED_URL})
    responses.add(responses.GET, PRESIGNED_URL, body=XLSX_BYTES, status=200)

    data = get_all_institutions_report(_client(), A_YEAR)

    assert data == XLSX_BYTES
    assert _report_accept_header() == XLSX_AUTHOR_SHARES_ACCEPT


@mock_aws
@responses.activate
def test_get_all_institutions_report_control_sends_control_accept_header():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, REPORT_URL, json={"uri": PRESIGNED_URL})
    responses.add(responses.GET, PRESIGNED_URL, body=XLSX_BYTES, status=200)

    data = get_all_institutions_report_control(_client(), A_YEAR)

    assert data == XLSX_BYTES
    assert _report_accept_header() == XLSX_AUTHOR_SHARES_CONTROL_ACCEPT


@mock_aws
@responses.activate
@patch("commands.services.scientific_index_api.time.sleep")
def test_report_polls_presigned_url_until_ready(_sleep):
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, REPORT_URL, json={"uri": PRESIGNED_URL})
    responses.add(responses.GET, PRESIGNED_URL, status=404)
    responses.add(responses.GET, PRESIGNED_URL, body=XLSX_BYTES, status=200)

    data = get_all_institutions_report(_client(), A_YEAR)

    assert data == XLSX_BYTES
    presigned_calls = [
        call for call in responses.calls if call.request.url == PRESIGNED_URL
    ]
    assert len(presigned_calls) == 2


@mock_aws
@responses.activate
def test_report_raises_on_non_404_presigned_error():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, REPORT_URL, json={"uri": PRESIGNED_URL})
    responses.add(responses.GET, PRESIGNED_URL, status=500)

    with pytest.raises(requests.HTTPError):
        get_all_institutions_report(_client(), A_YEAR)
