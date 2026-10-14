import json

import boto3
import pytest
from moto import mock_aws

from commands.services.approvals_api import (
    CREDENTIALS_SECRET_NAME,
    ApprovalsService,
    read_credentials,
    required_credentials_keys,
    validated_policy,
    write_credentials,
)

TABLE_NAME = "nva-approvals-master-pipelines-NvaHandleService-ABC123-nva-handle-service"
CUSTOMER_IDENTIFIER = "f8a1c0e2-3b4d-4a5e-9c7f-1d2e3f4a5b6c"
CREATED_DATE = "2026-09-08T09:11:19.123Z"
OAUTH2_SOURCE = {
    "baseUri": "https://api.dmp.no/",
    "authentication": {"type": "OAuth2ClientCredentials", "key": "api.dmp.no"},
}
OPEN_SOURCE = {
    "baseUri": "https://api.rekportalen.no/",
    "authentication": {"type": "NoAuthentication"},
}
CREDENTIALS = {
    "type": "OAuth2Credentials",
    "clientId": "id",
    "clientSecret": "secret",
    "accessTokenUrl": "https://auth.dmp.no/token",
    "scope": "dmp/read",
}


def _create_table() -> None:
    boto3.client("dynamodb").create_table(
        TableName=TABLE_NAME,
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


def _seed_policy(item: dict) -> None:
    boto3.resource("dynamodb").Table(TABLE_NAME).put_item(
        Item={
            "PK0": f"Customer:{CUSTOMER_IDENTIFIER}",
            "SK0": "IdentifierPolicy",
            "type": "IdentifierPolicy",
            "customerIdentifier": CUSTOMER_IDENTIFIER,
            "createdDate": CREATED_DATE,
            **item,
        }
    )


def test_validated_policy_normalizes_names_and_keeps_source_order():
    policy = validated_policy(
        {
            "allowedIdentifierNames": [" DMP", "Ctis", "dmp"],
            "sourceConfigs": [OAUTH2_SOURCE, OPEN_SOURCE],
        }
    )

    assert policy == {
        "allowedIdentifierNames": ["ctis", "dmp"],
        "sourceConfigs": [OAUTH2_SOURCE, OPEN_SOURCE],
    }
    assert required_credentials_keys(policy) == ["api.dmp.no"]


@pytest.mark.parametrize(
    "invalid_policy",
    [
        {"allowedIdentifierNames": "rek"},
        {"allowedIdentifierNames": {}},
        {"allowedIdentifierNames": ["rek/1"]},
        {"allowedIdentifierNames": [""]},
        {"allowedIdentifierNames": ["x" * 101]},
        {
            "sourceConfigs": [
                {"baseUri": "/relative", "authentication": {"type": "NoAuthentication"}}
            ]
        },
        {
            "sourceConfigs": [
                {"baseUri": "https://api.dmp.no/", "authentication": {"type": "Basic"}}
            ]
        },
        {"sourceConfigs": [{"baseUri": "https://api.dmp.no/"}]},
        {
            "sourceConfigs": [
                {
                    "baseUri": "https://api.dmp.no/",
                    "authentication": {"type": "OAuth2ClientCredentials"},
                }
            ]
        },
        {"sourceConfigs": ["https://api.dmp.no/"]},
    ],
)
def test_validated_policy_rejects_what_the_service_cannot_read(invalid_policy):
    with pytest.raises(ValueError):
        validated_policy(invalid_policy)


@mock_aws
def test_put_policy_keeps_attributes_owned_by_the_service():
    _create_table()
    _seed_policy({"allowedIdentifierNames": ["rek"]})

    ApprovalsService(boto3.Session(), TABLE_NAME).put_policy(
        CUSTOMER_IDENTIFIER,
        validated_policy(
            {"allowedIdentifierNames": ["dmp"], "sourceConfigs": [OPEN_SOURCE]}
        ),
    )

    assert ApprovalsService(boto3.Session(), TABLE_NAME).get_policy(
        CUSTOMER_IDENTIFIER
    ) == {
        "PK0": f"Customer:{CUSTOMER_IDENTIFIER}",
        "SK0": "IdentifierPolicy",
        "type": "IdentifierPolicy",
        "customerIdentifier": CUSTOMER_IDENTIFIER,
        "createdDate": CREATED_DATE,
        "allowedIdentifierNames": ["dmp"],
        "sourceConfigs": [OPEN_SOURCE],
    }


@mock_aws
def test_put_policy_omits_empty_lists_and_creates_missing_row():
    _create_table()
    service = ApprovalsService(boto3.Session(), TABLE_NAME)

    service.put_policy(CUSTOMER_IDENTIFIER, validated_policy({}))

    assert service.get_policy(CUSTOMER_IDENTIFIER) == {
        "PK0": f"Customer:{CUSTOMER_IDENTIFIER}",
        "SK0": "IdentifierPolicy",
        "type": "IdentifierPolicy",
        "customerIdentifier": CUSTOMER_IDENTIFIER,
    }


@mock_aws
def test_write_credentials_creates_secret_when_missing():
    write_credentials(boto3.Session(), {"api.dmp.no": CREDENTIALS})

    assert read_credentials(boto3.Session()) == {"api.dmp.no": CREDENTIALS}


@mock_aws
def test_write_credentials_keeps_the_other_keys():
    session = boto3.Session()
    session.client("secretsmanager").create_secret(
        Name=CREDENTIALS_SECRET_NAME,
        SecretString=json.dumps({"credentials": {"api.rek.no": CREDENTIALS}}),
    )

    write_credentials(session, {"api.dmp.no": CREDENTIALS})

    assert sorted(read_credentials(session)) == ["api.dmp.no", "api.rek.no"]
