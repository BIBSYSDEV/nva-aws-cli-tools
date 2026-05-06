import re

import boto3

from commands.services.user_models import Customer


def list_missing_customers(session: boto3.Session) -> list[dict]:
    dynamodb = session.resource("dynamodb")
    customers_table = dynamodb.Table(_get_table_name(session, "nva-customers"))
    users_table = dynamodb.Table(_get_table_name(session, "nva-users-and-roles"))

    customer_identifiers = _extract_customer_identifiers(customers_table)
    return _find_missing_customers(users_table, customer_identifiers)


def list_duplicate_customers(session: boto3.Session) -> list[dict]:
    dynamodb = session.resource("dynamodb")
    customers_table = dynamodb.Table(_get_table_name(session, "nva-customers"))
    return _find_duplicate_customers(customers_table)


def get_all_customers(session: boto3.Session) -> list[Customer]:
    dynamodb = session.resource("dynamodb")
    customers_table = dynamodb.Table(_get_table_name(session, "nva-customers"))
    raw_customers = _scan_table(customers_table)
    return [Customer.from_dynamodb(item) for item in raw_customers]


def build_customer_lookup(session: boto3.Session) -> dict[str, str]:
    return {
        customer.identifier: customer.name
        for customer in get_all_customers(session)
        if customer.identifier
    }


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


def _get_table_name(session: boto3.Session, name_prefix: str) -> str:
    response = session.client("dynamodb").list_tables()
    for table_name in response["TableNames"]:
        if table_name.startswith(name_prefix):
            return table_name
    raise ValueError(f"No table found with prefix {name_prefix!r}")
