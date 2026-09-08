import re

import boto3

from commands.services.aws_utils import find_table_name
from commands.services.user_models import Customer

CUSTOMERS_TABLE_NAME = "nva-customers"
USERS_TABLE_NAME = "nva-users-and-roles"


def list_missing_customers(session: boto3.Session) -> list[dict]:
    dynamodb = session.resource("dynamodb")
    customers_table = dynamodb.Table(find_table_name(session, CUSTOMERS_TABLE_NAME))
    users_table = dynamodb.Table(find_table_name(session, USERS_TABLE_NAME))

    customer_identifiers = _extract_customer_identifiers(customers_table)
    return _find_missing_customers(users_table, customer_identifiers)


def list_duplicate_customers(session: boto3.Session) -> list[dict]:
    dynamodb = session.resource("dynamodb")
    customers_table = dynamodb.Table(find_table_name(session, CUSTOMERS_TABLE_NAME))
    return _find_duplicate_customers(customers_table)


def get_all_customers(session: boto3.Session) -> list[Customer]:
    dynamodb = session.resource("dynamodb")
    customers_table = dynamodb.Table(find_table_name(session, CUSTOMERS_TABLE_NAME))
    raw_customers = _scan_table(customers_table)
    return [Customer.from_dynamodb(item) for item in raw_customers]


def build_customer_lookup(session: boto3.Session) -> dict[str, str]:
    return {
        customer.identifier: customer.name
        for customer in get_all_customers(session)
        if customer.identifier
    }


def search_customers(session: boto3.Session, search_term: str) -> list[Customer]:
    search_words = search_term.lower().split()
    matching_customers = [
        customer
        for customer in get_all_customers(session)
        if _matches_all_words(customer, search_words)
    ]
    return sorted(matching_customers, key=lambda customer: customer.name.lower())


def _matches_all_words(customer: Customer, search_words: list[str]) -> bool:
    searchable_text = _searchable_text(customer)
    return all(word in searchable_text for word in search_words)


def _searchable_text(customer: Customer) -> str:
    values = (
        customer.identifier,
        customer.name,
        customer.display_name,
        customer.short_name,
        customer.cristin_id,
        customer.feideOrganizationDomain,
        customer.cname,
    )
    return " ".join(value for value in values if value).lower()


def _find_duplicate_customers(customers_table) -> list[dict]:
    cristin_id_counts: dict[str, int] = {}
    matching_items = []
    for item in _scan_table(customers_table):
        if "cristinId" not in item:
            continue
        match = re.search(r"\d+", item["cristinId"])
        if not match:
            continue
        first_number = match.group()
        cristin_id_counts[first_number] = cristin_id_counts.get(first_number, 0) + 1
        if cristin_id_counts[first_number] >= 2:
            matching_items.append(item)
    return matching_items


def _scan_table(table) -> list[dict]:
    items = []
    response = table.scan()
    items.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response["Items"])
    return items


def _extract_customer_identifiers(customers_table) -> set[str]:
    return {customer["identifier"] for customer in _scan_table(customers_table)}


def _find_missing_customers(users_table, customer_identifiers: set[str]) -> list[dict]:
    missing_customers = []
    for user in _scan_table(users_table):
        if "institution" not in user:
            continue
        match = re.search(r"(?<=customer/).+", user["institution"])
        if not match:
            continue
        customer_id = match.group()
        if customer_id not in customer_identifiers:
            missing_customers.append(
                {
                    "PrimaryKeyHashKey": user["PrimaryKeyHashKey"],
                    "MissingCustomerId": customer_id,
                }
            )
    return missing_customers
