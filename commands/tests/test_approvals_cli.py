import json

import boto3
from click.testing import CliRunner
from moto import mock_aws

from cli import cli
from commands.services.approvals_api import CREDENTIALS_SECRET_NAME, read_credentials

APPROVALS_TABLE = (
    "nva-approvals-master-pipelines-NvaHandleService-ABC123-nva-handle-service"
)
CUSTOMERS_TABLE = "nva-customers-ABC123"
CUSTOMER_IDENTIFIER = "f8a1c0e2-3b4d-4a5e-9c7f-1d2e3f4a5b6c"
CREATED_DATE = "2026-09-08T09:11:19.123Z"
OAUTH2_SOURCE = {
    "baseUri": "https://api.dmp.no/",
    "authentication": {"type": "OAuth2ClientCredentials", "key": "api.dmp.no"},
}
CLIENT_SECRET = "s3cr3t"
CREDENTIALS_INPUT = f"id\n{CLIENT_SECRET}\nhttps://auth.dmp.no/token\ndmp/read\n"


def _create_approvals_table() -> None:
    boto3.client("dynamodb").create_table(
        TableName=APPROVALS_TABLE,
        KeySchema=[
            {"AttributeName": "PK0", "KeyType": "HASH"},
            {"AttributeName": "SK0", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK0", "AttributeType": "S"},
            {"AttributeName": "SK0", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )


def _create_customers_table(customers: dict[str, str]) -> None:
    boto3.client("dynamodb").create_table(
        TableName=CUSTOMERS_TABLE,
        KeySchema=[{"AttributeName": "identifier", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "identifier", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    table = boto3.resource("dynamodb").Table(CUSTOMERS_TABLE)
    for identifier, name in customers.items():
        table.put_item(Item={"identifier": identifier, "name": name})


def _seed_policy(**attributes) -> None:
    boto3.resource("dynamodb").Table(APPROVALS_TABLE).put_item(
        Item={
            "PK0": f"Customer:{CUSTOMER_IDENTIFIER}",
            "SK0": "IdentifierPolicy",
            "type": "IdentifierPolicy",
            "customerIdentifier": CUSTOMER_IDENTIFIER,
            "createdDate": CREATED_DATE,
            **attributes,
        }
    )


def _raw_policy() -> dict:
    return (
        boto3.resource("dynamodb")
        .Table(APPROVALS_TABLE)
        .get_item(
            Key={"PK0": f"Customer:{CUSTOMER_IDENTIFIER}", "SK0": "IdentifierPolicy"}
        )
        .get("Item", {})
    )


def _run(args: list[str], user_input: str | None = None):
    return CliRunner().invoke(
        cli, ["--quiet", "approvals", *args], input=user_input, env={"COLUMNS": "200"}
    )


def _edits(monkeypatch, policy: dict) -> None:
    monkeypatch.setattr(
        "commands.approvals.click.edit", lambda *_, **__: json.dumps(policy)
    )


@mock_aws
def test_policy_lists_names_sources_and_customer_names():
    _create_approvals_table()
    _create_customers_table({CUSTOMER_IDENTIFIER: "Sikt"})
    _seed_policy(allowedIdentifierNames=["ctis", "dmp"], sourceConfigs=[OAUTH2_SOURCE])
    boto3.resource("dynamodb").Table(APPROVALS_TABLE).put_item(
        Item={"PK0": "Approval:1", "SK0": "Approval:1", "type": "Approval"}
    )

    result = _run(["policy"])

    assert result.exit_code == 0, result.output
    assert "Sikt" in result.output
    assert "ctis, dmp" in result.output
    assert "https://api.dmp.no/ (OAuth2ClientCredentials: api.dmp.no)" in result.output
    assert "Total: 1 policy(ies)" in result.output


@mock_aws
def test_policy_edit_keeps_service_owned_attributes_after_confirmation(monkeypatch):
    _create_approvals_table()
    _seed_policy(allowedIdentifierNames=["rek"])
    _edits(monkeypatch, {"allowedIdentifierNames": ["REK ", "ctis"]})

    result = _run(["policy", CUSTOMER_IDENTIFIER], user_input="y\n")

    assert result.exit_code == 0, result.output
    assert APPROVALS_TABLE in result.output
    assert _raw_policy() == {
        "PK0": f"Customer:{CUSTOMER_IDENTIFIER}",
        "SK0": "IdentifierPolicy",
        "type": "IdentifierPolicy",
        "customerIdentifier": CUSTOMER_IDENTIFIER,
        "createdDate": CREATED_DATE,
        "allowedIdentifierNames": ["ctis", "rek"],
    }


@mock_aws
def test_policy_edit_writes_nothing_when_not_confirmed(monkeypatch):
    _create_approvals_table()
    _seed_policy(allowedIdentifierNames=["rek"])
    _edits(monkeypatch, {"allowedIdentifierNames": ["dmp"]})

    result = _run(["policy", CUSTOMER_IDENTIFIER], user_input="n\n")

    assert result.exit_code != 0
    assert _raw_policy()["allowedIdentifierNames"] == ["rek"]


@mock_aws
def test_policy_edit_asks_for_missing_credentials_and_writes_both(monkeypatch):
    _create_approvals_table()
    _seed_policy(allowedIdentifierNames=["dmp"])
    _edits(
        monkeypatch,
        {"allowedIdentifierNames": ["dmp"], "sourceConfigs": [OAUTH2_SOURCE]},
    )

    result = _run(["policy", CUSTOMER_IDENTIFIER], user_input=CREDENTIALS_INPUT + "y\n")

    assert result.exit_code == 0, result.output
    assert CLIENT_SECRET not in result.output
    assert _raw_policy()["sourceConfigs"] == [OAUTH2_SOURCE]
    assert read_credentials(boto3.Session())["api.dmp.no"] == {
        "type": "OAuth2Credentials",
        "clientId": "id",
        "clientSecret": CLIENT_SECRET,
        "accessTokenUrl": "https://auth.dmp.no/token",
        "scope": "dmp/read",
    }


@mock_aws
def test_credentials_keeps_other_keys_and_never_echoes_the_secret():
    session = boto3.Session()
    session.client("secretsmanager").create_secret(
        Name=CREDENTIALS_SECRET_NAME,
        SecretString=json.dumps({"credentials": {"api.rek.no": {"type": "x"}}}),
    )

    result = _run(["credentials", "api.dmp.no"], user_input=CREDENTIALS_INPUT + "y\n")

    assert result.exit_code == 0, result.output
    assert CLIENT_SECRET not in result.output
    assert sorted(read_credentials(session)) == ["api.dmp.no", "api.rek.no"]
