import boto3
import pytest
from moto import mock_aws

from commands.services.approvals_api import (
    IdentifierPolicy,
    IdentifierPolicyAlreadyExistsError,
    IdentifierPolicyNotFoundError,
    IdentifierPolicyService,
    normalize_identifier_names,
    parse_customer_identifier,
)
from commands.services.aws_utils import find_table_name

TABLE_NAME = "nva-approvals-master-pipelines-NvaHandleService-ABC123-nva-handle-service"
CUSTOMER_IDENTIFIER = "f8a1c0e2-3b4d-4a5e-9c7f-1d2e3f4a5b6c"
OTHER_CUSTOMER_IDENTIFIER = "0b6f7a1e-2c3d-4e5f-8a9b-0c1d2e3f4a5b"


def _create_approvals_table(name: str = TABLE_NAME) -> None:
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


def _put_raw_item(item: dict) -> None:
    boto3.resource("dynamodb").Table(TABLE_NAME).put_item(Item=item)


def _get_raw_item(customer_identifier: str) -> dict | None:
    response = (
        boto3.resource("dynamodb")
        .Table(TABLE_NAME)
        .get_item(
            Key={"PK0": f"Customer:{customer_identifier}", "SK0": "IdentifierPolicy"}
        )
    )
    return response.get("Item")


def test_parse_customer_identifier_accepts_uuid():
    assert parse_customer_identifier(CUSTOMER_IDENTIFIER) == CUSTOMER_IDENTIFIER


def test_parse_customer_identifier_accepts_customer_uri():
    uri = f"https://api.nva.unit.no/customer/{CUSTOMER_IDENTIFIER}"
    assert parse_customer_identifier(uri) == CUSTOMER_IDENTIFIER


def test_parse_customer_identifier_lowercases_uuid():
    assert parse_customer_identifier(CUSTOMER_IDENTIFIER.upper()) == CUSTOMER_IDENTIFIER


def test_parse_customer_identifier_rejects_non_uuid():
    with pytest.raises(ValueError, match="not a customer UUID"):
        parse_customer_identifier("not-a-uuid")


def test_normalize_identifier_names_trims_lowercases_dedupes_and_sorts():
    assert normalize_identifier_names([" DMP", "ctis", "dmp ", "Rek"]) == [
        "ctis",
        "dmp",
        "rek",
    ]


def test_normalize_identifier_names_rejects_blank_names():
    with pytest.raises(ValueError, match="must not be blank"):
        normalize_identifier_names(["ctis", "   "])


def test_to_dynamodb_matches_handle_service_layout():
    policy = IdentifierPolicy.create(CUSTOMER_IDENTIFIER, ["DMP", "ctis"])

    assert policy.to_dynamodb() == {
        "PK0": f"Customer:{CUSTOMER_IDENTIFIER}",
        "SK0": "IdentifierPolicy",
        "type": "IdentifierPolicy",
        "customerIdentifier": CUSTOMER_IDENTIFIER,
        "allowedIdentifierNames": ["ctis", "dmp"],
    }


def test_to_dynamodb_omits_empty_allowed_names_like_handle_service_does():
    policy = IdentifierPolicy.create(CUSTOMER_IDENTIFIER, [])

    assert "allowedIdentifierNames" not in policy.to_dynamodb()


def test_from_dynamodb_treats_missing_allowed_names_as_deny_all():
    policy = IdentifierPolicy.from_dynamodb(
        {
            "PK0": f"Customer:{CUSTOMER_IDENTIFIER}",
            "SK0": "IdentifierPolicy",
            "type": "IdentifierPolicy",
            "customerIdentifier": CUSTOMER_IDENTIFIER,
        }
    )

    assert policy.customer_identifier == CUSTOMER_IDENTIFIER
    assert policy.allowed_identifier_names == ()


def test_from_dynamodb_falls_back_to_partition_key_for_customer_identifier():
    policy = IdentifierPolicy.from_dynamodb(
        {"PK0": f"Customer:{CUSTOMER_IDENTIFIER}", "SK0": "IdentifierPolicy"}
    )

    assert policy.customer_identifier == CUSTOMER_IDENTIFIER


def test_with_names_adds_and_removes_normalized_names():
    policy = IdentifierPolicy.create(CUSTOMER_IDENTIFIER, ["ctis", "dmp"])

    updated = policy.with_names(names_to_add=["REK"], names_to_remove=[" Dmp "])

    assert updated.allowed_identifier_names == ("ctis", "rek")


@mock_aws
def test_find_table_name_matches_substring():
    _create_approvals_table()
    _create_approvals_table("nva-customers-something")

    assert find_table_name(boto3.Session(), "nva-approvals-") == TABLE_NAME


@mock_aws
def test_find_table_name_fails_when_nothing_matches():
    _create_approvals_table("nva-customers-something")

    with pytest.raises(ValueError, match="No DynamoDB table found"):
        find_table_name(boto3.Session(), "nva-approvals-")


@mock_aws
def test_find_table_name_fails_when_several_tables_match():
    _create_approvals_table("nva-approvals-stack-one")
    _create_approvals_table("nva-approvals-stack-two")

    with pytest.raises(ValueError, match="Several DynamoDB tables"):
        find_table_name(boto3.Session(), "nva-approvals-")


@mock_aws
def test_create_policy_writes_row_with_normalized_names():
    _create_approvals_table()
    service = IdentifierPolicyService(boto3.Session())

    policy = service.create_policy(CUSTOMER_IDENTIFIER, ["CTIS", "dmp"])

    assert policy.allowed_identifier_names == ("ctis", "dmp")
    assert _get_raw_item(CUSTOMER_IDENTIFIER) == {
        "PK0": f"Customer:{CUSTOMER_IDENTIFIER}",
        "SK0": "IdentifierPolicy",
        "type": "IdentifierPolicy",
        "customerIdentifier": CUSTOMER_IDENTIFIER,
        "allowedIdentifierNames": ["ctis", "dmp"],
    }


@mock_aws
def test_create_policy_refuses_to_overwrite_existing_policy():
    _create_approvals_table()
    service = IdentifierPolicyService(boto3.Session())
    service.create_policy(CUSTOMER_IDENTIFIER, ["ctis"])

    with pytest.raises(IdentifierPolicyAlreadyExistsError):
        service.create_policy(CUSTOMER_IDENTIFIER, ["dmp"])

    assert service.get_policy(CUSTOMER_IDENTIFIER) == IdentifierPolicy.create(
        CUSTOMER_IDENTIFIER, ["ctis"]
    )


@mock_aws
def test_get_policy_returns_none_when_missing():
    _create_approvals_table()
    service = IdentifierPolicyService(boto3.Session())

    assert service.get_policy(CUSTOMER_IDENTIFIER) is None


@mock_aws
def test_list_policies_only_returns_identifier_policy_rows_sorted_by_customer():
    _create_approvals_table()
    _put_raw_item(
        {
            "PK0": "Approval:some-approval",
            "SK0": "Approval",
            "type": "Approval",
        }
    )
    service = IdentifierPolicyService(boto3.Session())
    service.create_policy(CUSTOMER_IDENTIFIER, ["ctis"])
    service.create_policy(OTHER_CUSTOMER_IDENTIFIER, ["dmp"])

    policies = service.list_policies()

    assert [policy.customer_identifier for policy in policies] == [
        OTHER_CUSTOMER_IDENTIFIER,
        CUSTOMER_IDENTIFIER,
    ]


@mock_aws
def test_update_policy_adds_and_removes_names():
    _create_approvals_table()
    service = IdentifierPolicyService(boto3.Session())
    service.create_policy(CUSTOMER_IDENTIFIER, ["ctis", "dmp"])

    updated = service.update_policy(
        CUSTOMER_IDENTIFIER, names_to_add=["rek"], names_to_remove=["dmp"]
    )

    assert updated.allowed_identifier_names == ("ctis", "rek")
    assert service.get_policy(CUSTOMER_IDENTIFIER) == updated


@mock_aws
def test_update_policy_removing_every_name_stores_deny_all_row():
    _create_approvals_table()
    service = IdentifierPolicyService(boto3.Session())
    service.create_policy(CUSTOMER_IDENTIFIER, ["ctis"])

    service.update_policy(
        CUSTOMER_IDENTIFIER, names_to_add=[], names_to_remove=["ctis"]
    )

    raw_item = _get_raw_item(CUSTOMER_IDENTIFIER)
    assert raw_item is not None
    assert "allowedIdentifierNames" not in raw_item


@mock_aws
def test_update_policy_fails_when_policy_is_missing():
    _create_approvals_table()
    service = IdentifierPolicyService(boto3.Session())

    with pytest.raises(IdentifierPolicyNotFoundError):
        service.update_policy(
            CUSTOMER_IDENTIFIER, names_to_add=["ctis"], names_to_remove=[]
        )


@mock_aws
def test_delete_policy_removes_row():
    _create_approvals_table()
    service = IdentifierPolicyService(boto3.Session())
    service.create_policy(CUSTOMER_IDENTIFIER, ["ctis"])

    service.delete_policy(CUSTOMER_IDENTIFIER)

    assert _get_raw_item(CUSTOMER_IDENTIFIER) is None


@mock_aws
def test_delete_policy_fails_when_policy_is_missing():
    _create_approvals_table()
    service = IdentifierPolicyService(boto3.Session())

    with pytest.raises(IdentifierPolicyNotFoundError):
        service.delete_policy(CUSTOMER_IDENTIFIER)
