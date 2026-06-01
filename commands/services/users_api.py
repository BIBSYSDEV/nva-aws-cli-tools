from datetime import datetime, timezone
from urllib.parse import quote_plus

import boto3
import requests

from commands.services.api_client import ApiClient
from commands.services.user_models import User

SYSTEM_USER = "nva-backend@20754.0.0.0"


def search_users(session: boto3.Session, search_term: str) -> list[dict]:
    table = _users_table(session)
    search_words = search_term.split()

    response = table.scan()
    matching = _filter(response["Items"], search_words)
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        matching.extend(_filter(response["Items"], search_words))
    return matching


def get_all_users(session: boto3.Session) -> list[User]:
    table = _users_table(session)
    raw_users: list[dict] = []

    scan_kwargs = {
        "FilterExpression": "begins_with(PrimaryKeyHashKey, :prefix)",
        "ExpressionAttributeValues": {":prefix": "USER#"},
    }
    response = table.scan(**scan_kwargs)
    raw_users.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table.scan(
            **scan_kwargs, ExclusiveStartKey=response["LastEvaluatedKey"]
        )
        raw_users.extend(response["Items"])

    return [User.from_dynamodb(item) for item in raw_users]


def get_user_by_username(client: ApiClient, username: str) -> dict | None:
    response = requests.get(
        f"https://{client.api_domain}/users-roles/users/{quote_plus(username)}",
        headers={**client.auth_header(), "Accept": "application/json"},
    )
    if response.status_code == 404:
        return None
    if not response.ok:
        raise ValueError(
            f"Failed to retrieve user by username. Status code: {response.status_code} - {response.text}"
        )
    return response.json()


def add_user(client: ApiClient, person: dict) -> dict:
    response = requests.post(
        f"https://{client.api_domain}/users-roles/users",
        json=person,
        headers={**client.auth_header(), "Accept": "application/json"},
    )
    if not response.ok:
        raise ValueError(
            f"Failed to create user. Status code: {response.status_code} - {response.text}"
        )
    return response.json()


def update_user(client: ApiClient, user: dict) -> dict:
    response = requests.put(
        f"https://{client.api_domain}/users-roles/users/{quote_plus(user['username'])}",
        json=user,
        headers={**client.auth_header(), "Accept": "application/json"},
    )
    if not response.ok:
        raise ValueError(
            f"Failed to update user. Status code: {response.status_code} - {response.text}"
        )
    return response.json()


def approve_terms(session: boto3.Session, client: ApiClient, person_id: str) -> dict:
    terms_uri = _current_terms_uri(client)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f000Z")
    item = {
        "id": f"https://{client.api_domain}/cristin/person/{person_id}",
        "type": "TermsConditions",
        "created": timestamp,
        "modified": timestamp,
        "modifiedBy": SYSTEM_USER,
        "owner": SYSTEM_USER,
        "termsConditionsUri": terms_uri,
    }
    _terms_table(session).put_item(Item=item)
    return item


def _current_terms_uri(client: ApiClient) -> str:
    response = requests.get(
        f"https://{client.api_domain}/users-roles/terms-and-conditions/current"
    )
    if response.status_code != 200:
        raise ValueError(
            f"Failed to retrieve current terms and conditions URI. Status code: {response.status_code} - {response.text}"
        )
    terms_uri = response.json().get("termsConditionsUri")
    if not terms_uri:
        raise ValueError("Current terms and conditions URI not found.")
    return terms_uri


def _users_table(session: boto3.Session):
    return session.resource("dynamodb").Table(
        _table_name(session, "nva-users-and-roles")
    )


def _terms_table(session: boto3.Session):
    return session.resource("dynamodb").Table(
        _table_name(session, "terms-and-conditions")
    )


def _table_name(session: boto3.Session, prefix: str) -> str:
    response = session.client("dynamodb").list_tables()
    for table_name in response["TableNames"]:
        if table_name.startswith(prefix):
            return table_name
    raise ValueError(f"No table found with prefix {prefix!r}")


def _filter(items: list[dict], search_words: list[str]) -> list[dict]:
    matching = []
    for item in items:
        item_text = " ".join(str(value) for value in item.values())
        if all(word in item_text for word in search_words):
            matching.append(item)
    return matching
