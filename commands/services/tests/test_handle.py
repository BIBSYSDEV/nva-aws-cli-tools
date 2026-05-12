import csv
import json

import boto3
import pytest
import requests
import responses
from click.testing import CliRunner
from moto import mock_aws

from commands.handle import handle, DONE_CSV
from commands.services.publication_api import extract_publication_identifier
from commands.services.search_api import SearchApiService

SEARCH_URL = "https://api.example.org/search/resources"
HANDLE_URL = "https://api.example.org/handle/11250/2497055"
COGNITO_URL = "https://cognito.example.org/oauth2/token"

A_HANDLE = "11250/2497055"
A_PUBLICATION_ID = "https://api.example.org/publication/0185ca7e2245-63254c68-0000"
A_IDENTIFIER = "0185ca7e2245-63254c68-0000"


def _a_hit(handle_value: str = A_HANDLE) -> dict:
    return {
        "id": A_PUBLICATION_ID,
        "otherIdentifiers": {"handle": [f"https://hdl.handle.net/{handle_value}"]},
    }


@mock_aws
def _seed_ssm():
    ssm = boto3.client("ssm", region_name="eu-west-1")
    ssm.put_parameter(Name="/NVA/ApiDomain", Value="api.example.org", Type="String")
    ssm.put_parameter(
        Name="/NVA/ApplicationDomain", Value="nva.example.org", Type="String"
    )
    ssm.put_parameter(
        Name="/NVA/CognitoUri", Value="https://cognito.example.org", Type="String"
    )
    boto3.client("secretsmanager", region_name="eu-west-1").create_secret(
        Name="BackendCognitoClientCredentials",
        SecretString=json.dumps(
            {"backendClientId": "id", "backendClientSecret": "secret"}
        ),
    )


def test_extract_publication_identifier():
    assert (
        extract_publication_identifier("https://api.nva.unit.no/publication/abc-123")
        == "abc-123"
    )
    assert (
        extract_publication_identifier("https://api.nva.unit.no/publication/abc-123/")
        == "abc-123"
    )


def test_hit_contains_handle_matches():
    service = SearchApiService.__new__(SearchApiService)
    hit = _a_hit("11250/2497055")
    assert service._hit_contains_handle(hit, "11250/2497055") is True


def test_hit_contains_handle_no_match():
    service = SearchApiService.__new__(SearchApiService)
    hit = _a_hit("11250/9999999")
    assert service._hit_contains_handle(hit, "11250/2497055") is False


def test_hit_contains_handle_no_prefix_false_positive():
    service = SearchApiService.__new__(SearchApiService)
    hit = _a_hit("11250/24970550000")
    assert service._hit_contains_handle(hit, "11250/2497055") is False


def test_hit_contains_handle_empty():
    service = SearchApiService.__new__(SearchApiService)
    hit = {"id": A_PUBLICATION_ID, "otherIdentifiers": {"handle": []}}
    assert service._hit_contains_handle(hit, "11250/2497055") is False


@mock_aws
@responses.activate
def test_find_by_handle_returns_matching_hit():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit()], "totalHits": 1})

    result = SearchApiService(None).find_by_handle(A_HANDLE)

    assert len(result) == 1
    assert result[0]["id"] == A_PUBLICATION_ID


@mock_aws
@responses.activate
def test_find_by_handle_filters_non_matching_hit():
    _seed_ssm()
    wrong_hit = _a_hit("11250/9999999")
    responses.add(responses.GET, SEARCH_URL, json={"hits": [wrong_hit], "totalHits": 1})

    result = SearchApiService(None).find_by_handle(A_HANDLE)

    assert result == []


@mock_aws
@responses.activate
def test_update_handle_raises_on_error_response(tmp_path):
    from commands.services.handle_api import HandleApiService

    _seed_ssm()
    responses.add(
        responses.POST, COGNITO_URL, json={"access_token": "token", "expires_in": 3600}
    )
    responses.add(responses.PUT, HANDLE_URL, status=404)

    service = HandleApiService(None)
    with pytest.raises(requests.exceptions.HTTPError):
        service.set_handle(A_HANDLE, "https://nva.example.org/registration/abc")


@mock_aws
@responses.activate
def test_redirect_to_nva_dry_run(tmp_path):
    _seed_ssm()
    responses.add(
        responses.POST, COGNITO_URL, json={"access_token": "token", "expires_in": 3600}
    )
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit()], "totalHits": 1})

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(
            handle, ["redirect-to-nva", "--dry-run", A_HANDLE], obj=_ctx()
        )

    assert result.exit_code == 0
    assert "DRY-RUN" in result.output
    assert A_IDENTIFIER in result.output
    assert len([c for c in responses.calls if c.request.method == "PUT"]) == 0


@mock_aws
@responses.activate
def test_redirect_to_nva_updates_handle(tmp_path):
    _seed_ssm()
    responses.add(
        responses.POST, COGNITO_URL, json={"access_token": "token", "expires_in": 3600}
    )
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit()], "totalHits": 1})
    responses.add(responses.PUT, HANDLE_URL, json={"status": "ok"})

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(handle, ["redirect-to-nva", A_HANDLE], obj=_ctx())
        csv_rows = _read_done_csv()

    assert result.exit_code == 0
    assert "UPDATED" in result.output
    put_calls = [c for c in responses.calls if c.request.method == "PUT"]
    assert len(put_calls) == 1
    assert A_IDENTIFIER in put_calls[0].request.body.decode()
    assert len(csv_rows) == 1
    assert csv_rows[0]["handle"] == A_HANDLE
    assert csv_rows[0]["status"] == "ok"


@mock_aws
@responses.activate
def test_redirect_to_nva_skips_when_no_hit(tmp_path):
    _seed_ssm()
    responses.add(
        responses.POST, COGNITO_URL, json={"access_token": "token", "expires_in": 3600}
    )
    responses.add(responses.GET, SEARCH_URL, json={"hits": [], "totalHits": 0})

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(handle, ["redirect-to-nva", A_HANDLE], obj=_ctx())
        csv_rows = _read_done_csv()

    assert result.exit_code == 0
    assert "SKIP" in result.output
    assert csv_rows[0]["status"] == "skipped"


@mock_aws
@responses.activate
def test_redirect_to_nva_skips_already_processed(tmp_path):
    _seed_ssm()
    responses.add(
        responses.POST, COGNITO_URL, json={"access_token": "token", "expires_in": 3600}
    )
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit()], "totalHits": 1})
    responses.add(responses.PUT, HANDLE_URL, json={"status": "ok"})

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        runner.invoke(handle, ["redirect-to-nva", A_HANDLE], obj=_ctx())
        result = runner.invoke(handle, ["redirect-to-nva", A_HANDLE], obj=_ctx())

    assert "already processed" in result.output
    put_calls = [c for c in responses.calls if c.request.method == "PUT"]
    assert len(put_calls) == 1


def _read_done_csv() -> list:
    try:
        with open(DONE_CSV, newline="") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def _ctx():
    from commands.utils import AppContext

    return AppContext(
        log_level=0, profile=None, session=boto3.Session(region_name="eu-west-1")
    )
