import csv
import json
import click
import sys
import logging
from collections import Counter
from operator import itemgetter

from rich.console import Console
from rich.table import Table

from commands.utils import AppContext
from commands.services.users_api import UsersAndRolesService
from commands.services.user_models import User
from commands.services.aws_utils import prettify
from commands.services.external_user import ExternalUserService
from commands.services.user_export import UserExportService

logger = logging.getLogger(__name__)


@click.group()
@click.pass_obj
def users(ctx: AppContext):
    pass


@users.command(help="Search users by user values")
@click.argument("search_term", required=True, nargs=-1)
@click.pass_obj
def search(ctx: AppContext, search_term: str) -> None:
    search_term = " ".join(search_term)
    result = UsersAndRolesService(ctx.profile).search(search_term)
    click.echo(prettify(result))


@users.command(help="Add user")
@click.argument("user_data", type=click.File("r"), default=sys.stdin)
@click.pass_obj
def add_user(ctx: AppContext, user_data: str) -> None:
    if user_data.isatty():
        user_data_json = sys.stdin.read()
    else:
        user_data_json = user_data.read()
    user = json.loads(user_data_json)
    result = UsersAndRolesService(ctx.profile).add_user(user)
    click.echo(prettify(result))


@users.command(help="Approve user terms by passing cristin person ID (e.g. 2009968)")
@click.argument("user_id", required=True)
@click.pass_obj
def approve_terms(ctx: AppContext, user_id: str) -> None:
    result = UsersAndRolesService(ctx.profile).approve_terms(user_id)
    click.echo(prettify(result))


@users.command(help="Add external API user")
@click.option(
    "-c",
    "--customer",
    required=True,
    help="Customer UUID. e.g. bb3d0c0c-5065-4623-9b98-5810983c2478",
)
@click.option(
    "-i",
    "--intended_purpose",
    required=True,
    help="The intended purpose. e.g. oslomet-thesis-integration",
)
@click.option(
    "-s",
    "--scopes",
    required=True,
    help="Comma-separated list of scopes without whitespace, e.g., https://api.nva.unit.no/scopes/third-party/publication-read,https://api.nva.unit.no/scopes/third-party/publication-upsert",
)
@click.option(
    "--shortname",
    required=False,
    default=None,
    help="Override the org shortname used in client name and acting user (e.g. ntnu). Needed when customer lacks shortName.",
)
@click.pass_obj
def create_external(
    ctx: AppContext,
    customer: str,
    intended_purpose: str,
    scopes: str,
    shortname: str | None,
) -> None:
    external_user = ExternalUserService(ctx.profile).create(
        customer, intended_purpose, scopes.split(","), shortname
    )
    external_user.save_to_file()
    click.echo(prettify(external_user.client_data))


@users.command(help="Export all users and their roles to Excel")
@click.option(
    "-o",
    "--output",
    help="Output filename (default: users-{profile}-YYYY-MM-DD-HHMMSS.xlsx)",
)
@click.option(
    "--exclude-only-roles",
    help="Comma-separated list of role names. Excludes users who have ONLY these roles and no other roles (e.g., 'Creator')",
)
@click.option(
    "--include-roles",
    help="Comma-separated list of role names to include in export (only these roles will be exported)",
)
@click.pass_obj
def export_roles(
    ctx: AppContext, output: str, exclude_only_roles: str, include_roles: str
) -> None:
    if exclude_only_roles and include_roles:
        click.echo(
            "Error: Cannot use both --exclude-only-roles and --include-roles at the same time"
        )
        return

    excluded_roles_list = (
        [role.strip() for role in exclude_only_roles.split(",")]
        if exclude_only_roles
        else None
    )
    included_roles_list = (
        [role.strip() for role in include_roles.split(",")] if include_roles else None
    )

    if excluded_roles_list:
        logger.info(
            f"Excluding users with ONLY roles: {', '.join(excluded_roles_list)}"
        )
    if included_roles_list:
        logger.info(
            f"Including only users with roles: {', '.join(included_roles_list)}"
        )

    logger.info("Fetching all users from DynamoDB...")
    logger.info("Fetching customer data for institution names...")

    service = UserExportService(ctx.profile)
    result = service.export_to_excel(
        output_filename=output,
        exclude_only_roles=excluded_roles_list,
        include_roles=included_roles_list,
    )

    logger.info(
        f"Found {result.total_users} users, exported {result.exported_users} users."
    )
    logger.info(f"Excel file saved to: {result.filename}")


def _count_users_per_role(
    users: list[User], role_filter: set[str] | None
) -> list[tuple[str, int]]:
    role_counts: Counter = Counter(
        role.name
        for user in users
        for role in user.roles
        if role.name and (role_filter is None or role.name in role_filter)
    )
    sorted_counts = sorted(role_counts.items())
    sorted_counts.sort(key=itemgetter(1), reverse=True)
    return sorted_counts


@users.command(help="Show number of users per role")
@click.option(
    "--roles",
    help="Comma-separated list of role names to include (default: all roles)",
)
@click.option("--csv-output", is_flag=True, help="Output as CSV instead of table")
@click.pass_obj
def role_summary(ctx: AppContext, roles: str, csv_output: bool) -> None:
    role_filter = {role.strip() for role in roles.split(",")} if roles else None

    all_users = UsersAndRolesService(ctx.profile).get_all_users()

    sorted_counts = _count_users_per_role(all_users, role_filter)

    if csv_output:
        writer = csv.writer(sys.stdout)
        writer.writerow(["Role", "Number of users"])
        writer.writerows(sorted_counts)
        return

    console = Console()
    table = Table(
        show_header=True,
        header_style="bold cyan",
        show_lines=True,
        title=f"[bold magenta]Role summary ({len(all_users)} users total)[/bold magenta]",
    )
    table.add_column("Role", style="bold")
    table.add_column("Number of users", justify="right")

    for role_name, count in sorted_counts:
        table.add_row(role_name, str(count))

    console.print(table)
