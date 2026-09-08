import json

import boto3
from click.testing import CliRunner
from moto import mock_aws

from cli import cli

APPROVALS_TABLE = (
    "nva-approvals-master-pipelines-NvaHandleService-ABC123-nva-handle-service"
)
CUSTOMERS_TABLE = "nva-customers-ABC123"
CUSTOMER_IDENTIFIER = "f8a1c0e2-3b4d-4a5e-9c7f-1d2e3f4a5b6c"
OTHER_CUSTOMER_IDENTIFIER = "0b6f7a1e-2c3d-4e5f-8a9b-0c1d2e3f4a5b"


def _create_approvals_table(name: str = APPROVALS_TABLE) -> None:
    boto3.client("dynamodb").create_table(
        TableName=name,
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


def _seed_policy(customer_identifier: str, allowed_names: list[str]) -> None:
    boto3.resource("dynamodb").Table(APPROVALS_TABLE).put_item(
        Item={
            "PK0": f"Customer:{customer_identifier}",
            "SK0": "IdentifierPolicy",
            "type": "IdentifierPolicy",
            "customerIdentifier": customer_identifier,
            "allowedIdentifierNames": allowed_names,
        }
    )


def _seed_approval(
    approval_identifier: str, handle: str, name: str, value: str
) -> None:
    approval_key = f"Approval:{approval_identifier}"
    handle_key = f"Handle:{handle}"
    identifier_key = f"Identifier:{name}#{value}"
    table = boto3.resource("dynamodb").Table(APPROVALS_TABLE)
    table.put_item(
        Item={
            "PK0": approval_key,
            "SK0": approval_key,
            "PK1": approval_key,
            "SK1": approval_key,
            "PK2": handle_key,
            "SK2": handle_key,
            "type": "Approval",
            "identifier": approval_identifier,
            "source": "https://example.org/source/1",
        }
    )
    table.put_item(
        Item={
            "PK0": handle_key,
            "SK0": handle_key,
            "PK1": approval_key,
            "SK1": approval_key,
            "PK2": handle_key,
            "SK2": handle_key,
            "type": "Handle",
            "uri": handle,
        }
    )
    table.put_item(
        Item={
            "PK0": identifier_key,
            "SK0": identifier_key,
            "PK1": approval_key,
            "SK1": approval_key,
            "PK2": handle_key,
            "SK2": handle_key,
            "type": "Identifier",
            "name": name,
            "value": value,
        }
    )


def _count_rows() -> int:
    return boto3.resource("dynamodb").Table(APPROVALS_TABLE).scan()["Count"]


def _raw_policy(customer_identifier: str) -> dict | None:
    response = (
        boto3.resource("dynamodb")
        .Table(APPROVALS_TABLE)
        .get_item(
            Key={"PK0": f"Customer:{customer_identifier}", "SK0": "IdentifierPolicy"}
        )
    )
    return response.get("Item")


def _run(args: list[str], user_input: str | None = None):
    return CliRunner().invoke(
        cli, ["--quiet", "approvals", "policies", *args], input=user_input
    )


def _table_row(output: str, needle: str) -> list[str]:
    matching_lines = [line for line in output.splitlines() if needle in line]
    assert matching_lines, f"No table row containing '{needle}' in output:\n{output}"
    cells = [cell.strip() for cell in matching_lines[0].split("│")]
    return [cell for cell in cells if cell]


@mock_aws
def test_list_shows_policies_with_customer_names():
    _create_approvals_table()
    _create_customers_table({CUSTOMER_IDENTIFIER: "Sikt"})
    _seed_policy(CUSTOMER_IDENTIFIER, ["ctis", "dmp"])
    _seed_policy(OTHER_CUSTOMER_IDENTIFIER, [])

    result = _run(["list"])

    assert result.exit_code == 0, result.output
    assert _table_row(result.output, CUSTOMER_IDENTIFIER) == [
        "Sikt",
        CUSTOMER_IDENTIFIER,
        "ctis, dmp",
    ]
    assert _table_row(result.output, OTHER_CUSTOMER_IDENTIFIER) == [
        "?",
        OTHER_CUSTOMER_IDENTIFIER,
        "(none - denies all)",
    ]
    assert "Total: 2 policy(ies)" in result.output


@mock_aws
def test_list_ignores_approval_handle_and_identifier_rows():
    _create_approvals_table()
    _seed_approval(
        "3d235e16-5fd5-4a7e-8eb6-ac53f298e65e",
        "https://hdl.handle.net/11250.1/12345",
        "REK",
        "2024/123",
    )
    _seed_policy(CUSTOMER_IDENTIFIER, ["rek"])

    result = _run(["list", "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [
        {"customerIdentifier": CUSTOMER_IDENTIFIER, "allowedIdentifierNames": ["rek"]}
    ]


@mock_aws
def test_list_still_works_when_customers_table_is_missing():
    _create_approvals_table()
    _seed_policy(CUSTOMER_IDENTIFIER, ["ctis"])

    result = _run(["list"])

    assert result.exit_code == 0, result.output
    assert _table_row(result.output, CUSTOMER_IDENTIFIER) == [
        "?",
        CUSTOMER_IDENTIFIER,
        "ctis",
    ]


@mock_aws
def test_list_json_outputs_policies():
    _create_approvals_table()
    _seed_policy(CUSTOMER_IDENTIFIER, ["dmp", "ctis"])

    result = _run(["list", "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [
        {
            "customerIdentifier": CUSTOMER_IDENTIFIER,
            "allowedIdentifierNames": ["ctis", "dmp"],
        }
    ]


@mock_aws
def test_list_reports_when_no_policies_exist():
    _create_approvals_table()

    result = _run(["list"])

    assert result.exit_code == 0, result.output
    assert "No identifier policies found" in result.output


@mock_aws
def test_list_fails_when_approvals_table_is_missing():
    result = _run(["list"])

    assert result.exit_code == 1
    assert "No DynamoDB table found containing 'nva-approvals-'" in result.output


@mock_aws
def test_table_option_disambiguates_between_several_stacks():
    _create_approvals_table("nva-approvals-feature-one")
    _create_approvals_table("nva-approvals-feature-two")

    ambiguous = _run(["list", "--json"])
    specific = _run(["list", "--json", "--table", "feature-two"])

    assert ambiguous.exit_code == 1
    assert "Several DynamoDB tables contain 'nva-approvals-'" in ambiguous.output
    assert specific.exit_code == 0, specific.output
    assert json.loads(specific.output) == []


@mock_aws
def test_get_outputs_policy_as_json():
    _create_approvals_table()
    _seed_policy(CUSTOMER_IDENTIFIER, ["ctis"])

    result = _run(["get", f"https://api.nva.unit.no/customer/{CUSTOMER_IDENTIFIER}"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "customerIdentifier": CUSTOMER_IDENTIFIER,
        "allowedIdentifierNames": ["ctis"],
    }


@mock_aws
def test_get_fails_when_policy_is_missing():
    _create_approvals_table()

    result = _run(["get", CUSTOMER_IDENTIFIER])

    assert result.exit_code == 1
    assert f"No identifier policy found for customer {CUSTOMER_IDENTIFIER}" in (
        result.output
    )


@mock_aws
def test_add_creates_policy_with_normalized_names():
    _create_approvals_table()

    result = _run(["add", CUSTOMER_IDENTIFIER, "CTIS", "dmp", " Dmp "])

    assert result.exit_code == 0, result.output
    assert (
        f"CREATED identifier policy for customer {CUSTOMER_IDENTIFIER}: ctis, dmp"
        in (result.output)
    )
    assert _raw_policy(CUSTOMER_IDENTIFIER) == {
        "PK0": f"Customer:{CUSTOMER_IDENTIFIER}",
        "SK0": "IdentifierPolicy",
        "type": "IdentifierPolicy",
        "customerIdentifier": CUSTOMER_IDENTIFIER,
        "allowedIdentifierNames": ["ctis", "dmp"],
    }


@mock_aws
def test_add_fails_when_policy_already_exists():
    _create_approvals_table()
    _seed_policy(CUSTOMER_IDENTIFIER, ["ctis"])

    result = _run(["add", CUSTOMER_IDENTIFIER, "dmp"])

    assert result.exit_code == 1
    assert "already exists" in result.output
    raw_policy = _raw_policy(CUSTOMER_IDENTIFIER)
    assert raw_policy is not None
    assert raw_policy["allowedIdentifierNames"] == ["ctis"]


@mock_aws
def test_add_requires_at_least_one_identifier_name():
    _create_approvals_table()

    result = _run(["add", CUSTOMER_IDENTIFIER])

    assert result.exit_code == 2
    assert _raw_policy(CUSTOMER_IDENTIFIER) is None


@mock_aws
def test_add_rejects_blank_identifier_name():
    _create_approvals_table()

    result = _run(["add", CUSTOMER_IDENTIFIER, "ctis", "  "])

    assert result.exit_code == 1
    assert "must not be blank" in result.output
    assert _raw_policy(CUSTOMER_IDENTIFIER) is None


@mock_aws
def test_add_rejects_invalid_customer_identifier():
    _create_approvals_table()

    result = _run(["add", "not-a-customer", "ctis"])

    assert result.exit_code == 2
    assert "not a customer UUID" in result.output


@mock_aws
def test_update_adds_and_removes_names():
    _create_approvals_table()
    _seed_policy(CUSTOMER_IDENTIFIER, ["ctis", "dmp"])

    result = _run(["update", CUSTOMER_IDENTIFIER, "--add", "REK", "--remove", "dmp"])

    assert result.exit_code == 0, result.output
    assert (
        f"UPDATED identifier policy for customer {CUSTOMER_IDENTIFIER}: ctis, rek"
        in (result.output)
    )
    raw_policy = _raw_policy(CUSTOMER_IDENTIFIER)
    assert raw_policy is not None
    assert raw_policy["allowedIdentifierNames"] == ["ctis", "rek"]


@mock_aws
def test_update_requires_add_or_remove():
    _create_approvals_table()
    _seed_policy(CUSTOMER_IDENTIFIER, ["ctis"])

    result = _run(["update", CUSTOMER_IDENTIFIER])

    assert result.exit_code == 2
    assert "at least one --add or --remove" in result.output


@mock_aws
def test_update_fails_when_policy_is_missing():
    _create_approvals_table()

    result = _run(["update", CUSTOMER_IDENTIFIER, "--add", "ctis"])

    assert result.exit_code == 1
    assert f"No identifier policy found for customer {CUSTOMER_IDENTIFIER}" in (
        result.output
    )
    assert _raw_policy(CUSTOMER_IDENTIFIER) is None


@mock_aws
def test_delete_asks_for_confirmation_and_deletes_when_confirmed():
    _create_approvals_table()
    _seed_policy(CUSTOMER_IDENTIFIER, ["ctis"])

    result = _run(["delete", CUSTOMER_IDENTIFIER], user_input="y\n")

    assert result.exit_code == 0, result.output
    assert "Delete identifier policy" in result.output
    assert f"DELETED identifier policy for customer {CUSTOMER_IDENTIFIER}" in (
        result.output
    )
    assert _raw_policy(CUSTOMER_IDENTIFIER) is None


@mock_aws
def test_delete_keeps_policy_when_not_confirmed():
    _create_approvals_table()
    _seed_policy(CUSTOMER_IDENTIFIER, ["ctis"])

    result = _run(["delete", CUSTOMER_IDENTIFIER], user_input="n\n")

    assert result.exit_code == 1
    assert "Aborted" in result.output
    assert _raw_policy(CUSTOMER_IDENTIFIER) is not None


@mock_aws
def test_delete_with_yes_skips_confirmation():
    _create_approvals_table()
    _seed_policy(CUSTOMER_IDENTIFIER, ["ctis"])

    result = _run(["delete", CUSTOMER_IDENTIFIER, "--yes"])

    assert result.exit_code == 0, result.output
    assert "Delete identifier policy" not in result.output
    assert _raw_policy(CUSTOMER_IDENTIFIER) is None


@mock_aws
def test_delete_leaves_approval_rows_untouched():
    _create_approvals_table()
    _seed_approval(
        "3d235e16-5fd5-4a7e-8eb6-ac53f298e65e",
        "https://hdl.handle.net/11250.1/12345",
        "REK",
        "2024/123",
    )
    _seed_policy(CUSTOMER_IDENTIFIER, ["rek"])
    assert _count_rows() == 4

    result = _run(["delete", CUSTOMER_IDENTIFIER, "--yes"])

    assert result.exit_code == 0, result.output
    assert _raw_policy(CUSTOMER_IDENTIFIER) is None
    assert _count_rows() == 3


@mock_aws
def test_delete_fails_when_policy_is_missing():
    _create_approvals_table()

    result = _run(["delete", CUSTOMER_IDENTIFIER, "--yes"])

    assert result.exit_code == 1
    assert f"No identifier policy found for customer {CUSTOMER_IDENTIFIER}" in (
        result.output
    )
