import json

import boto3
from click.testing import CliRunner
from moto import mock_aws

from cli import cli


def _create_table(name: str, items: list[dict]) -> None:
    boto3.client("dynamodb").create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "identifier", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "identifier", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    table = boto3.resource("dynamodb").Table(name)
    for item in items:
        table.put_item(Item=item)


@mock_aws
def test_list_missing_reports_users_referencing_unknown_customers():
    _create_table(
        "nva-customers",
        [{"identifier": "known-customer-1"}, {"identifier": "known-customer-2"}],
    )
    _create_table(
        "nva-users-and-roles",
        [
            {"identifier": "alice", "PrimaryKeyHashKey": "alice", "institution": "https://api.example.org/customer/known-customer-1"},
            {"identifier": "bob", "PrimaryKeyHashKey": "bob", "institution": "https://api.example.org/customer/missing-customer"},
        ],
    )

    result = CliRunner().invoke(cli, ["--quiet", "customers", "list-missing"])

    assert result.exit_code == 0, result.exception
    payload = json.loads(result.output)
    assert payload == [{"PrimaryKeyHashKey": "bob", "MissingCustomerId": "missing-customer"}]


@mock_aws
def test_list_duplicate_reports_customers_sharing_a_cristin_id():
    _create_table(
        "nva-customers",
        [
            {"identifier": "first", "cristinId": "https://api.example.org/cristin/organization/12345"},
            {"identifier": "second", "cristinId": "https://api.example.org/cristin/organization/12345"},
            {"identifier": "third", "cristinId": "https://api.example.org/cristin/organization/99999"},
        ],
    )

    result = CliRunner().invoke(cli, ["--quiet", "customers", "list-duplicate"])

    assert result.exit_code == 0, result.exception
    payload = json.loads(result.output)
    duplicate_identifiers = {item["identifier"] for item in payload}
    assert duplicate_identifiers == {"second"}
