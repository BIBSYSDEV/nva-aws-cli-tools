import json

import boto3
import responses
from click.testing import CliRunner
from moto import mock_aws

from cli import cli
from commands.services.sws import (
    API_ENDPOINT_NON_PROD,
    CREDENTIALS_SECRET_NAME,
    TOKEN_ENDPOINT_NON_PROD,
)


def _seed_credentials() -> None:
    boto3.client("secretsmanager").create_secret(
        Name=CREDENTIALS_SECRET_NAME,
        SecretString=json.dumps({"username": "client-id", "password": "client-secret"}),
    )


@mock_aws
@responses.activate
def test_get_mappings_returns_index_mapping_as_json():
    _seed_credentials()
    responses.add(
        responses.POST, TOKEN_ENDPOINT_NON_PROD, json={"access_token": "fresh"}
    )
    responses.add(
        responses.GET,
        f"{API_ENDPOINT_NON_PROD}/resources/_mapping",
        json={"resources": {"mappings": {"properties": {"title": {"type": "text"}}}}},
    )

    result = CliRunner().invoke(cli, ["--quiet", "sws", "get-mappings", "resources"])

    assert result.exit_code == 0, result.exception
    payload = json.loads(result.output)
    assert payload["resources"]["mappings"]["properties"]["title"]["type"] == "text"


@mock_aws
@responses.activate
def test_get_mappings_exits_non_zero_when_index_does_not_exist():
    _seed_credentials()
    responses.add(
        responses.POST, TOKEN_ENDPOINT_NON_PROD, json={"access_token": "fresh"}
    )
    responses.add(
        responses.GET,
        f"{API_ENDPOINT_NON_PROD}/missing/_mapping",
        json={"error": "index not found"},
        status=404,
    )

    result = CliRunner().invoke(cli, ["--quiet", "sws", "get-mappings", "missing"])

    assert result.exit_code == 1
    assert "Failed to retrieve mappings" in result.output
