from __future__ import annotations

import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from commands.services.aws_utils import find_table_name

DEFAULT_TABLE_NAME_SUBSTRING = "nva-approvals-"

PARTITION_KEY = "PK0"
SORT_KEY = "SK0"
TYPE_ATTRIBUTE = "type"
CUSTOMER_IDENTIFIER_ATTRIBUTE = "customerIdentifier"
ALLOWED_IDENTIFIER_NAMES_ATTRIBUTE = "allowedIdentifierNames"

IDENTIFIER_POLICY_TYPE = "IdentifierPolicy"
CUSTOMER_KEY_PREFIX = "Customer:"

CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailedException"


class IdentifierPolicyError(Exception):
    pass


class IdentifierPolicyNotFoundError(IdentifierPolicyError):
    def __init__(self, customer_identifier: str) -> None:
        super().__init__(
            f"No identifier policy found for customer {customer_identifier}"
        )


class IdentifierPolicyAlreadyExistsError(IdentifierPolicyError):
    def __init__(self, customer_identifier: str) -> None:
        super().__init__(
            f"An identifier policy already exists for customer {customer_identifier}"
        )


def parse_customer_identifier(value: str) -> str:
    candidate = value.strip().rstrip("/").rsplit("/", 1)[-1]
    try:
        return str(uuid.UUID(candidate))
    except ValueError as error:
        raise ValueError(
            f"{value!r} is not a customer UUID or a customer URI ending in a UUID"
        ) from error


def normalize_identifier_names(names: Iterable[str]) -> list[str]:
    normalized_names = {name.strip().lower() for name in names}
    if "" in normalized_names:
        raise ValueError("Identifier names must not be blank")
    return sorted(normalized_names)


def customer_key(customer_identifier: str) -> str:
    return f"{CUSTOMER_KEY_PREFIX}{customer_identifier}"


def policy_primary_key(customer_identifier: str) -> dict[str, str]:
    return {
        PARTITION_KEY: customer_key(customer_identifier),
        SORT_KEY: IDENTIFIER_POLICY_TYPE,
    }


@dataclass(frozen=True)
class IdentifierPolicy:
    customer_identifier: str
    allowed_identifier_names: tuple[str, ...]

    @classmethod
    def create(
        cls, customer_identifier: str, allowed_identifier_names: Iterable[str]
    ) -> IdentifierPolicy:
        return cls(
            customer_identifier=customer_identifier,
            allowed_identifier_names=tuple(
                normalize_identifier_names(allowed_identifier_names)
            ),
        )

    @classmethod
    def from_dynamodb(cls, item: dict[str, Any]) -> IdentifierPolicy:
        customer_identifier = item.get(
            CUSTOMER_IDENTIFIER_ATTRIBUTE
        ) or _customer_identifier_from_key(item.get(PARTITION_KEY, ""))
        raw_names = item.get(ALLOWED_IDENTIFIER_NAMES_ATTRIBUTE) or []
        return cls(
            customer_identifier=str(customer_identifier),
            allowed_identifier_names=tuple(sorted(str(name) for name in raw_names)),
        )

    def to_dynamodb(self) -> dict[str, Any]:
        item: dict[str, Any] = {
            **policy_primary_key(self.customer_identifier),
            TYPE_ATTRIBUTE: IDENTIFIER_POLICY_TYPE,
            CUSTOMER_IDENTIFIER_ATTRIBUTE: self.customer_identifier,
        }
        if self.allowed_identifier_names:
            item[ALLOWED_IDENTIFIER_NAMES_ATTRIBUTE] = list(
                self.allowed_identifier_names
            )
        return item

    def to_json_dict(self) -> dict[str, Any]:
        return {
            CUSTOMER_IDENTIFIER_ATTRIBUTE: self.customer_identifier,
            ALLOWED_IDENTIFIER_NAMES_ATTRIBUTE: list(self.allowed_identifier_names),
        }

    def with_names(
        self, names_to_add: Iterable[str], names_to_remove: Iterable[str]
    ) -> IdentifierPolicy:
        resulting_names = set(self.allowed_identifier_names)
        resulting_names.update(normalize_identifier_names(names_to_add))
        resulting_names.difference_update(normalize_identifier_names(names_to_remove))
        return IdentifierPolicy(
            customer_identifier=self.customer_identifier,
            allowed_identifier_names=tuple(sorted(resulting_names)),
        )


class IdentifierPolicyService:
    def __init__(
        self,
        session: boto3.Session,
        table_name_substring: str = DEFAULT_TABLE_NAME_SUBSTRING,
    ) -> None:
        self.table_name = find_table_name(session, table_name_substring)
        self.table = session.resource("dynamodb").Table(self.table_name)

    def list_policies(self) -> list[IdentifierPolicy]:
        policies = [
            IdentifierPolicy.from_dynamodb(item) for item in self._scan_policy_items()
        ]
        return sorted(policies, key=lambda policy: policy.customer_identifier)

    def get_policy(self, customer_identifier: str) -> IdentifierPolicy | None:
        response = self.table.get_item(Key=policy_primary_key(customer_identifier))
        item = response.get("Item")
        return IdentifierPolicy.from_dynamodb(item) if item else None

    def create_policy(
        self, customer_identifier: str, allowed_identifier_names: Iterable[str]
    ) -> IdentifierPolicy:
        policy = IdentifierPolicy.create(customer_identifier, allowed_identifier_names)
        try:
            self.table.put_item(
                Item=policy.to_dynamodb(),
                ConditionExpression=Attr(PARTITION_KEY).not_exists(),
            )
        except ClientError as error:
            if _is_conditional_check_failure(error):
                raise IdentifierPolicyAlreadyExistsError(customer_identifier) from error
            raise
        return policy

    def update_policy(
        self,
        customer_identifier: str,
        names_to_add: Iterable[str],
        names_to_remove: Iterable[str],
    ) -> IdentifierPolicy:
        existing_policy = self.get_policy(customer_identifier)
        if existing_policy is None:
            raise IdentifierPolicyNotFoundError(customer_identifier)
        updated_policy = existing_policy.with_names(names_to_add, names_to_remove)
        try:
            self.table.put_item(
                Item=updated_policy.to_dynamodb(),
                ConditionExpression=Attr(PARTITION_KEY).exists(),
            )
        except ClientError as error:
            if _is_conditional_check_failure(error):
                raise IdentifierPolicyNotFoundError(customer_identifier) from error
            raise
        return updated_policy

    def delete_policy(self, customer_identifier: str) -> None:
        try:
            self.table.delete_item(
                Key=policy_primary_key(customer_identifier),
                ConditionExpression=Attr(PARTITION_KEY).exists(),
            )
        except ClientError as error:
            if _is_conditional_check_failure(error):
                raise IdentifierPolicyNotFoundError(customer_identifier) from error
            raise

    def _scan_policy_items(self) -> list[dict[str, Any]]:
        scan_kwargs: dict[str, Any] = {
            "FilterExpression": Attr(SORT_KEY).eq(IDENTIFIER_POLICY_TYPE)
        }
        items: list[dict[str, Any]] = []
        response = self.table.scan(**scan_kwargs)
        items.extend(response.get("Items", []))
        while "LastEvaluatedKey" in response:
            response = self.table.scan(
                **scan_kwargs, ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            items.extend(response.get("Items", []))
        return items


def _customer_identifier_from_key(partition_key: str) -> str:
    return str(partition_key).removeprefix(CUSTOMER_KEY_PREFIX)


def _is_conditional_check_failure(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") == CONDITIONAL_CHECK_FAILED
