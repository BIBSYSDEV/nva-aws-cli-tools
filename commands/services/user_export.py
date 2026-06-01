from datetime import datetime

import boto3
import polars as pl

from commands.services.customers_api import build_customer_lookup
from commands.services.user_models import ExportResult, User
from commands.services.users_api import get_all_users


def export_users_to_excel(
    session: boto3.Session,
    *,
    output_filename: str | None = None,
    exclude_only_roles: list[str] | None = None,
    include_roles: list[str] | None = None,
) -> ExportResult:
    all_users = get_all_users(session)
    customer_lookup = build_customer_lookup(session)

    excluded_only_roles_set = set(exclude_only_roles) if exclude_only_roles else set()
    included_roles_set = set(include_roles) if include_roles else set()

    filtered_users = _filter_users(
        all_users, excluded_only_roles_set, included_roles_set
    )

    if not output_filename:
        timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        output_filename = f"users-{timestamp}.xlsx"

    _create_excel_file(filtered_users, customer_lookup, output_filename)

    return ExportResult(
        total_users=len(all_users),
        exported_users=len(filtered_users),
        filename=output_filename,
    )


def _filter_users(
    all_users: list[User],
    excluded_only_roles: set[str],
    included_roles: set[str],
) -> list[User]:
    filtered = []
    for user in all_users:
        user_role_names = {role.name for role in user.roles if role.name}
        if excluded_only_roles:
            if not (user_role_names and user_role_names.issubset(excluded_only_roles)):
                filtered.append(user)
        elif included_roles:
            if user_role_names.intersection(included_roles):
                filtered.append(user)
        else:
            filtered.append(user)
    return filtered


def _create_excel_file(
    users: list[User], customer_lookup: dict[str, str], output_filename: str
) -> None:
    excel_rows = [user.to_excel_row(customer_lookup) for user in users]
    rows = [row.to_list() for row in excel_rows]
    df = pl.DataFrame(rows, schema=User.ExcelRow.headers(), orient="row")
    df.write_excel(output_filename, worksheet="Users and Roles", autofit=True)
