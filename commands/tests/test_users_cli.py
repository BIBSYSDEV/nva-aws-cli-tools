import json

import boto3
import responses
from click.testing import CliRunner
from moto import mock_aws

from cli import cli


def _seed_nva_api_credentials() -> None:
    ssm = boto3.client("ssm")
    ssm.put_parameter(Name="/NVA/ApiDomain", Value="api.example.org", Type="String")
    ssm.put_parameter(
        Name="/NVA/CognitoUri", Value="https://cognito.example.org", Type="String"
    )
    boto3.client("secretsmanager").create_secret(
        Name="BackendCognitoClientCredentials",
        SecretString=json.dumps(
            {"backendClientId": "id", "backendClientSecret": "secret"}
        ),
    )


def _seed_token_endpoint() -> None:
    responses.add(
        responses.POST,
        "https://cognito.example.org/oauth2/token",
        json={"access_token": "fresh", "expires_in": 3600},
    )


def _create_users_table(users: list[dict]) -> None:
    boto3.client("dynamodb").create_table(
        TableName="nva-users-and-roles",
        KeySchema=[{"AttributeName": "PrimaryKeyHashKey", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "PrimaryKeyHashKey", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table = boto3.resource("dynamodb").Table("nva-users-and-roles")
    for user in users:
        table.put_item(Item=user)


@mock_aws
def test_search_returns_users_matching_all_search_words():
    _create_users_table(
        [
            {
                "PrimaryKeyHashKey": "USER#alice",
                "username": "alice",
                "email": "alice@example.org",
            },
            {
                "PrimaryKeyHashKey": "USER#bob",
                "username": "bob",
                "email": "bob@other.org",
            },
        ]
    )

    result = CliRunner().invoke(cli, ["--quiet", "users", "search", "alice", "example"])

    assert result.exit_code == 0, result.exception
    payload = json.loads(result.output)
    assert len(payload) == 1
    assert payload[0]["username"] == "alice"


@mock_aws
@responses.activate
def test_add_user_posts_to_users_roles_endpoint():
    _seed_nva_api_credentials()
    _seed_token_endpoint()
    responses.add(
        responses.POST,
        "https://api.example.org/users-roles/users",
        json={"username": "alice@org", "cristinIdentifier": "12345"},
    )

    payload = {
        "cristinIdentifier": "12345",
        "customerId": "https://api.example.org/customer/abc",
    }
    result = CliRunner().invoke(
        cli, ["--quiet", "users", "add-user", "-"], input=json.dumps(payload)
    )

    assert result.exit_code == 0, result.exception
    created = json.loads(result.output)
    assert created["username"] == "alice@org"
    post_call = next(
        call
        for call in responses.calls
        if call.request.method == "POST" and "users-roles/users" in call.request.url
    )
    assert json.loads(post_call.request.body) == payload


@mock_aws
def test_role_summary_csv_counts_roles_grouped_by_name():
    _create_users_table(
        [
            {
                "PrimaryKeyHashKey": "USER#alice",
                "username": "alice",
                "roles": [
                    {"type": "Role", "name": "Creator"},
                    {"type": "Role", "name": "Editor"},
                ],
            },
            {
                "PrimaryKeyHashKey": "USER#bob",
                "username": "bob",
                "roles": [{"type": "Role", "name": "Creator"}],
            },
            {
                "PrimaryKeyHashKey": "USER#carol",
                "username": "carol",
                "roles": [{"type": "Role", "name": "Editor"}],
            },
        ]
    )

    result = CliRunner().invoke(
        cli, ["--quiet", "users", "role-summary", "--csv-output"]
    )

    assert result.exit_code == 0, result.exception
    assert "Creator,2" in result.output
    assert "Editor,2" in result.output


@mock_aws
def test_role_summary_filters_to_given_roles():
    _create_users_table(
        [
            {
                "PrimaryKeyHashKey": "USER#alice",
                "username": "alice",
                "roles": [
                    {"type": "Role", "name": "Creator"},
                    {"type": "Role", "name": "Editor"},
                ],
            },
            {
                "PrimaryKeyHashKey": "USER#bob",
                "username": "bob",
                "roles": [{"type": "Role", "name": "Creator"}],
            },
        ]
    )

    result = CliRunner().invoke(
        cli, ["--quiet", "users", "role-summary", "--csv-output", "--roles", "Creator"]
    )

    assert result.exit_code == 0, result.exception
    assert "Creator,2" in result.output
    assert "Editor" not in result.output


@mock_aws
@responses.activate
def test_create_external_writes_credentials_file_and_uses_shortname_override(
    monkeypatch, tmp_path
):
    monkeypatch.chdir(tmp_path)
    _seed_nva_api_credentials()
    _seed_token_endpoint()
    customer_uuid = "bb3d0c0c-5065-4623-9b98-5810983c2478"
    responses.add(
        responses.GET,
        f"https://api.example.org/customer/{customer_uuid}",
        json={
            "id": f"https://api.example.org/customer/{customer_uuid}",
            "cristinId": "https://api.example.org/cristin/organization/1234",
            "shortName": "DefaultName",
        },
    )
    responses.add(
        responses.POST,
        "https://api.example.org/users-roles/external-clients",
        json={
            "clientId": "abc",
            "clientSecret": "def",
            "clientUrl": "https://token.example.org",
        },
    )

    result = CliRunner().invoke(
        cli,
        [
            "--quiet",
            "users",
            "create-external",
            "--customer",
            customer_uuid,
            "--intended_purpose",
            "thesis-integration",
            "--scopes",
            "https://api.example.org/scopes/publication-read",
            "--shortname",
            "MyOrg",
        ],
    )

    assert result.exit_code == 0, result.exception
    credentials_file = tmp_path / "myorg-thesis-integration-credentials.json"
    assert credentials_file.exists()
    saved = json.loads(credentials_file.read_text())
    assert saved["clientId"] == "abc"
    assert saved["clientName"] == "myorg-thesis-integration-integration"
    assert saved["actingUser"] == "thesis-integration-integration@myorg"


@mock_aws
@responses.activate
def test_create_external_falls_back_to_customer_shortname_when_not_overridden(
    monkeypatch, tmp_path
):
    monkeypatch.chdir(tmp_path)
    _seed_nva_api_credentials()
    _seed_token_endpoint()
    customer_uuid = "bb3d0c0c-5065-4623-9b98-5810983c2478"
    responses.add(
        responses.GET,
        f"https://api.example.org/customer/{customer_uuid}",
        json={
            "id": f"https://api.example.org/customer/{customer_uuid}",
            "cristinId": "https://api.example.org/cristin/organization/1234",
            "shortName": "NTNU",
        },
    )
    responses.add(
        responses.POST,
        "https://api.example.org/users-roles/external-clients",
        json={
            "clientId": "abc",
            "clientSecret": "def",
            "clientUrl": "https://token.example.org",
        },
    )

    result = CliRunner().invoke(
        cli,
        [
            "--quiet",
            "users",
            "create-external",
            "--customer",
            customer_uuid,
            "--intended_purpose",
            "thesis-integration",
            "--scopes",
            "https://api.example.org/scopes/publication-read",
        ],
    )

    assert result.exit_code == 0, result.exception
    assert (tmp_path / "ntnu-thesis-integration-credentials.json").exists()
