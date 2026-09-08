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
            {
                "identifier": "alice",
                "PrimaryKeyHashKey": "alice",
                "institution": "https://api.example.org/customer/known-customer-1",
            },
            {
                "identifier": "bob",
                "PrimaryKeyHashKey": "bob",
                "institution": "https://api.example.org/customer/missing-customer",
            },
        ],
    )

    result = CliRunner().invoke(cli, ["--quiet", "customers", "list-missing"])

    assert result.exit_code == 0, result.exception
    payload = json.loads(result.output)
    assert payload == [
        {"PrimaryKeyHashKey": "bob", "MissingCustomerId": "missing-customer"}
    ]


@mock_aws
def test_list_duplicate_reports_customers_sharing_a_cristin_id():
    _create_table(
        "nva-customers",
        [
            {
                "identifier": "first",
                "cristinId": "https://api.example.org/cristin/organization/12345",
            },
            {
                "identifier": "second",
                "cristinId": "https://api.example.org/cristin/organization/12345",
            },
            {
                "identifier": "third",
                "cristinId": "https://api.example.org/cristin/organization/99999",
            },
        ],
    )

    result = CliRunner().invoke(cli, ["--quiet", "customers", "list-duplicate"])

    assert result.exit_code == 0, result.exception
    payload = json.loads(result.output)
    duplicate_identifiers = {item["identifier"] for item in payload}
    assert duplicate_identifiers == {"second"}


SIKT = {
    "identifier": "sikt-id",
    "name": "Sikt",
    "displayName": "Sikt AS",
    "shortName": "SIKT",
    "cristinId": "https://api.nva.unit.no/cristin/organization/20754.0.0.0",
    "feideOrganizationDomain": "sikt.no",
}
UIO = {
    "identifier": "uio-id",
    "name": "UiO",
    "displayName": "Universitetet i Oslo",
    "cristinId": "https://api.nva.unit.no/cristin/organization/185.90.0.0",
    "feideOrganizationDomain": "uio.no",
}


def _table_row(output: str, needle: str) -> list[str]:
    matching_lines = [line for line in output.splitlines() if needle in line]
    assert matching_lines, f"No table row containing '{needle}' in output:\n{output}"
    cells = [cell.strip() for cell in matching_lines[0].split("│")]
    return [cell for cell in cells if cell]


@mock_aws
def test_search_matches_case_insensitively_and_renders_table():
    _create_table("nva-customers", [SIKT, UIO])

    result = CliRunner().invoke(cli, ["--quiet", "customers", "search", "sIkT"])

    assert result.exit_code == 0, result.exception
    assert _table_row(result.output, "sikt-id") == [
        "Sikt AS",
        "sikt-id",
        "20754.0.0.0",
        "sikt.no",
    ]
    assert "uio-id" not in result.output
    assert "Total: 1 customer(s)" in result.output


@mock_aws
def test_search_requires_every_word_to_match():
    _create_table("nva-customers", [SIKT, UIO])

    result = CliRunner().invoke(cli, ["--quiet", "customers", "search", "i", "oslo"])

    assert result.exit_code == 0, result.exception
    assert "uio-id" in result.output
    assert "sikt-id" not in result.output


@mock_aws
def test_search_matches_feide_domain_and_cristin_id():
    _create_table("nva-customers", [SIKT, UIO])

    by_feide = CliRunner().invoke(cli, ["--quiet", "customers", "search", "uio.no"])
    by_cristin = CliRunner().invoke(cli, ["--quiet", "customers", "search", "20754"])

    assert "uio-id" in by_feide.output
    assert "sikt-id" not in by_feide.output
    assert "sikt-id" in by_cristin.output
    assert "uio-id" not in by_cristin.output


@mock_aws
def test_search_json_outputs_matching_customers():
    _create_table("nva-customers", [SIKT, UIO])

    result = CliRunner().invoke(
        cli, ["--quiet", "customers", "search", "sikt", "--json"]
    )

    assert result.exit_code == 0, result.exception
    payload = json.loads(result.output)
    assert [customer["identifier"] for customer in payload] == ["sikt-id"]
    assert payload[0]["display_name"] == "Sikt AS"
    assert payload[0]["feideOrganizationDomain"] == "sikt.no"


@mock_aws
def test_search_reports_when_nothing_matches():
    _create_table("nva-customers", [SIKT, UIO])

    result = CliRunner().invoke(cli, ["--quiet", "customers", "search", "ntnu"])

    assert result.exit_code == 0, result.exception
    assert "No customers matching 'ntnu'" in result.output
