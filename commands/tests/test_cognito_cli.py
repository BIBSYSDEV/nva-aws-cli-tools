import json

import boto3
from click.testing import CliRunner
from moto import mock_aws

from cli import cli
from commands.services.cognito_api import USER_POOL_ID_PARAMETER


def _seed_user_pool(username: str, email: str) -> None:
    cognito = boto3.client("cognito-idp")
    pool_id = cognito.create_user_pool(PoolName="nva-test")["UserPool"]["Id"]
    cognito.admin_create_user(
        UserPoolId=pool_id,
        Username=username,
        UserAttributes=[{"Name": "email", "Value": email}],
    )
    boto3.client("ssm").put_parameter(Name=USER_POOL_ID_PARAMETER, Value=pool_id, Type="String")


@mock_aws
def test_cognito_search_prints_matching_user_as_json():
    _seed_user_pool(username="alice", email="alice@example.org")

    result = CliRunner().invoke(cli, ["--quiet", "cognito", "search", "alice"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload[0]["Username"] == "alice"


@mock_aws
def test_cognito_search_prints_null_when_no_match():
    _seed_user_pool(username="alice", email="alice@example.org")

    result = CliRunner().invoke(cli, ["--quiet", "cognito", "search", "nobody"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) is None
