"""Pattern: end-to-end CLI integration test using `click.testing.CliRunner`.

`runner.invoke(cli, [...])` exercises the real entry point: argument parsing,
the root group, the subcommand, the service layer, and the AWS calls.
Combined with `@mock_aws` this is the highest-fidelity test we can write
short of a real AWS account, and the failure modes it catches (wrong option
name, missing `@click.pass_obj`, output going to stderr, exit code wrong)
are not visible from service-level tests. Reach for this for golden-path
coverage per command. Use service-level tests for edge cases."""

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
    runner = CliRunner()

    result = runner.invoke(cli, ["--quiet", "cognito", "search", "alice"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload[0]["Username"] == "alice"


@mock_aws
def test_cognito_search_prints_null_when_no_match():
    _seed_user_pool(username="alice", email="alice@example.org")
    runner = CliRunner()

    result = runner.invoke(cli, ["--quiet", "cognito", "search", "missing"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) is None
