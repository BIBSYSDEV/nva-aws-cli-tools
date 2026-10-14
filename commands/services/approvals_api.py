from __future__ import annotations

import json
import re
import uuid
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from commands.services.aws_utils import find_table_name

DEFAULT_TABLE_NAME_SUBSTRING = "nva-approvals-"
CREDENTIALS_SECRET_NAME = "ApprovalCredentials"

PARTITION_KEY = "PK0"
SORT_KEY = "SK0"
TYPE_ATTRIBUTE = "type"
CUSTOMER_IDENTIFIER_ATTRIBUTE = "customerIdentifier"
NAMES_ATTRIBUTE = "allowedIdentifierNames"
SOURCE_CONFIGS_ATTRIBUTE = "sourceConfigs"
BASE_URI_ATTRIBUTE = "baseUri"
AUTHENTICATION_ATTRIBUTE = "authentication"
CREDENTIALS_KEY_ATTRIBUTE = "key"
CREDENTIALS_ATTRIBUTE = "credentials"

IDENTIFIER_POLICY_TYPE = "IdentifierPolicy"
CUSTOMER_KEY_PREFIX = "Customer:"
NO_AUTHENTICATION = "NoAuthentication"
OAUTH2_AUTHENTICATION = "OAuth2ClientCredentials"
OAUTH2_CREDENTIALS_TYPE = "OAuth2Credentials"
AUTHENTICATION_TYPES = (NO_AUTHENTICATION, OAUTH2_AUTHENTICATION)

CLIENT_SECRET_FIELD = "clientSecret"
SCOPE_FIELD = "scope"
CREDENTIALS_FIELDS = ("clientId", CLIENT_SECRET_FIELD, "accessTokenUrl", SCOPE_FIELD)

NAME_PATTERN = re.compile(r"[a-z0-9_-]{1,100}")
RESOURCE_NOT_FOUND = "ResourceNotFoundException"
EMPTY_POLICY: dict[str, Any] = {NAMES_ATTRIBUTE: [], SOURCE_CONFIGS_ATTRIBUTE: []}


def parse_customer_identifier(value: str) -> str:
    candidate = value.strip().rstrip("/").rsplit("/", 1)[-1]
    try:
        return str(uuid.UUID(candidate))
    except ValueError as error:
        raise ValueError(
            f"{value!r} is not a customer UUID or a customer URI ending in a UUID"
        ) from error


def policy_primary_key(customer_identifier: str) -> dict[str, str]:
    return {
        PARTITION_KEY: f"{CUSTOMER_KEY_PREFIX}{customer_identifier}",
        SORT_KEY: IDENTIFIER_POLICY_TYPE,
    }


def validated_policy(edited_policy: dict[str, Any]) -> dict[str, Any]:
    if not all(
        isinstance(edited_policy.get(attribute, []), list) for attribute in EMPTY_POLICY
    ):
        raise ValueError(
            f"{NAMES_ATTRIBUTE} and {SOURCE_CONFIGS_ATTRIBUTE} must be lists"
        )
    names = sorted(
        {str(name).strip().lower() for name in edited_policy.get(NAMES_ATTRIBUTE) or []}
    )
    for name in names:
        if not NAME_PATTERN.fullmatch(name):
            raise ValueError(
                f"{name!r} is not a usable identifier name: expected {NAME_PATTERN.pattern}"
            )
    source_configs = list(edited_policy.get(SOURCE_CONFIGS_ATTRIBUTE) or [])
    if not all(isinstance(source, dict) for source in source_configs):
        raise ValueError(f"Every {SOURCE_CONFIGS_ATTRIBUTE} entry must be an object")
    for source_config in source_configs:
        _validate_source_config(source_config)
    return {NAMES_ATTRIBUTE: names, SOURCE_CONFIGS_ATTRIBUTE: source_configs}


def required_credentials_keys(policy: dict[str, Any]) -> list[str]:
    return [
        source_config[AUTHENTICATION_ATTRIBUTE][CREDENTIALS_KEY_ATTRIBUTE]
        for source_config in policy[SOURCE_CONFIGS_ATTRIBUTE]
        if source_config[AUTHENTICATION_ATTRIBUTE][TYPE_ATTRIBUTE]
        == OAUTH2_AUTHENTICATION
    ]


def read_credentials(session: boto3.Session) -> dict[str, Any]:
    try:
        secret = session.client("secretsmanager").get_secret_value(
            SecretId=CREDENTIALS_SECRET_NAME
        )
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != RESOURCE_NOT_FOUND:
            raise
        return {}
    try:
        return json.loads(secret["SecretString"]).get(CREDENTIALS_ATTRIBUTE) or {}
    except json.JSONDecodeError as error:
        raise ValueError(f"{CREDENTIALS_SECRET_NAME} is not JSON: {error}") from error


def write_credentials(
    session: boto3.Session, new_credentials: dict[str, dict[str, str]]
) -> None:
    merged = {CREDENTIALS_ATTRIBUTE: read_credentials(session) | new_credentials}
    client = session.client("secretsmanager")
    try:
        client.put_secret_value(
            SecretId=CREDENTIALS_SECRET_NAME, SecretString=json.dumps(merged)
        )
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != RESOURCE_NOT_FOUND:
            raise
        client.create_secret(
            Name=CREDENTIALS_SECRET_NAME, SecretString=json.dumps(merged)
        )


def _with_lists(item: dict[str, Any]) -> dict[str, Any]:
    """Rows written as DynamoDB string sets come back as Python sets, which
    neither JSON nor the editor can show; the service reads sets and lists alike."""
    return {
        attribute: sorted(value) if isinstance(value, set) else value
        for attribute, value in item.items()
    }


def _validate_source_config(source_config: dict[str, Any]) -> None:
    base_uri = urlparse(str(source_config.get(BASE_URI_ATTRIBUTE)))
    if not (base_uri.scheme and base_uri.netloc):
        raise ValueError(
            f"{BASE_URI_ATTRIBUTE} must be an absolute URI, got "
            f"{source_config.get(BASE_URI_ATTRIBUTE)!r}"
        )
    authentication = source_config.get(AUTHENTICATION_ATTRIBUTE) or {}
    authentication_type = authentication.get(TYPE_ATTRIBUTE)
    if authentication_type not in AUTHENTICATION_TYPES:
        raise ValueError(
            f"{AUTHENTICATION_ATTRIBUTE}.{TYPE_ATTRIBUTE} must be one of "
            f"{', '.join(AUTHENTICATION_TYPES)}, got {authentication_type!r}"
        )
    if (
        authentication_type == OAUTH2_AUTHENTICATION
        and not str(authentication.get(CREDENTIALS_KEY_ATTRIBUTE) or "").strip()
    ):
        raise ValueError(
            f"{OAUTH2_AUTHENTICATION} needs a {CREDENTIALS_KEY_ATTRIBUTE} naming "
            f"credentials in the {CREDENTIALS_SECRET_NAME} secret"
        )


class ApprovalsService:
    def __init__(
        self,
        session: boto3.Session,
        table_name_substring: str = DEFAULT_TABLE_NAME_SUBSTRING,
    ) -> None:
        self.table_name = find_table_name(session, table_name_substring)
        self.table = session.resource("dynamodb").Table(self.table_name)

    def list_policies(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        scan_arguments: dict[str, Any] = {}
        while True:
            response = self.table.scan(**scan_arguments)
            items.extend(response.get("Items", []))
            if "LastEvaluatedKey" not in response:
                break
            scan_arguments["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        policies = [
            _with_lists(item)
            for item in items
            if item.get(SORT_KEY) == IDENTIFIER_POLICY_TYPE
        ]
        return sorted(policies, key=lambda item: item.get(PARTITION_KEY, ""))

    def get_policy(self, customer_identifier: str) -> dict[str, Any] | None:
        response = self.table.get_item(Key=policy_primary_key(customer_identifier))
        item = response.get("Item")
        return _with_lists(item) if item else None

    def put_policy(self, customer_identifier: str, policy: dict[str, Any]) -> None:
        item = self.get_policy(customer_identifier) or {}
        item |= policy_primary_key(customer_identifier)
        item[TYPE_ATTRIBUTE] = IDENTIFIER_POLICY_TYPE
        item[CUSTOMER_IDENTIFIER_ATTRIBUTE] = customer_identifier
        for attribute, value in policy.items():
            item[attribute] = value
            if not value:
                item.pop(attribute)
        self.table.put_item(Item=item)
