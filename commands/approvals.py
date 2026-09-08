import functools
import logging

import click
from botocore.exceptions import BotoCoreError, ClientError
from rich.console import Console
from rich.table import Table

from commands.services.approvals_api import (
    DEFAULT_TABLE_NAME_SUBSTRING,
    IdentifierPolicy,
    IdentifierPolicyError,
    IdentifierPolicyService,
    parse_customer_identifier,
)
from commands.services.aws_utils import prettify
from commands.services.customers_api import build_customer_lookup
from commands.utils import AppContext

logger = logging.getLogger(__name__)

UNKNOWN_CUSTOMER_NAME = "?"
DENY_ALL_LABEL = "(none - denies all)"


class CustomerIdentifierType(click.ParamType):
    name = "customer"

    def convert(self, value, param, ctx) -> str:
        if not isinstance(value, str):
            return value
        try:
            return parse_customer_identifier(value)
        except ValueError as error:
            self.fail(str(error), param, ctx)


CUSTOMER_IDENTIFIER = CustomerIdentifierType()


def _table_option(func):
    return click.option(
        "--table",
        "table_name_substring",
        default=DEFAULT_TABLE_NAME_SUBSTRING,
        show_default=True,
        help="Substring of the approvals DynamoDB table name. Make it more specific when several stacks match.",
    )(func)


def _handle_errors(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (IdentifierPolicyError, ValueError) as error:
            raise click.ClickException(str(error))

    return wrapper


@click.group()
@click.pass_obj
def approvals(ctx: AppContext) -> None:
    """Manage data owned by nva-handle-service (approvals table)."""


@approvals.group()
@click.pass_obj
def policies(ctx: AppContext) -> None:
    """Manage identifier policies: which identifier names each customer may use.

    The approvals table also holds the approval records themselves (Approval,
    Handle and Identifier rows). These commands only read and write the
    IdentifierPolicy rows and never touch approvals.

    There is at most one policy per customer. Names are stored lower-cased,
    matching how nva-handle-service normalizes them.
    """


@policies.command(name="list")
@_table_option
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Print JSON instead of a table",
)
@click.pass_obj
@_handle_errors
def list_policies(ctx: AppContext, table_name_substring: str, as_json: bool) -> None:
    """List all identifier policies."""
    service = IdentifierPolicyService(ctx.session, table_name_substring)
    identifier_policies = service.list_policies()
    if as_json:
        click.echo(prettify([policy.to_json_dict() for policy in identifier_policies]))
        return
    _render_policies(identifier_policies, _customer_names(ctx), service.table_name)


@policies.command()
@click.argument("customer_identifier", type=CUSTOMER_IDENTIFIER)
@_table_option
@click.pass_obj
@_handle_errors
def get(ctx: AppContext, customer_identifier: str, table_name_substring: str) -> None:
    """Show the identifier policy for one customer as JSON."""
    service = IdentifierPolicyService(ctx.session, table_name_substring)
    policy = service.get_policy(customer_identifier)
    if policy is None:
        raise click.ClickException(
            f"No identifier policy found for customer {customer_identifier}"
        )
    click.echo(prettify(policy.to_json_dict()))


@policies.command()
@click.argument("customer_identifier", type=CUSTOMER_IDENTIFIER)
@click.argument("identifier_names", nargs=-1, required=True)
@_table_option
@click.pass_obj
@_handle_errors
def add(
    ctx: AppContext,
    customer_identifier: str,
    identifier_names: tuple[str, ...],
    table_name_substring: str,
) -> None:
    """Create the identifier policy for a customer (fails if one already exists).

    CUSTOMER_IDENTIFIER is the customer UUID or customer URI. IDENTIFIER_NAMES are
    the allowed identifier names, e.g. ctis dmp rek.
    """
    service = IdentifierPolicyService(ctx.session, table_name_substring)
    policy = service.create_policy(customer_identifier, identifier_names)
    click.echo(
        f"CREATED identifier policy for customer {customer_identifier}: "
        f"{_format_names(policy)}"
    )


@policies.command()
@click.argument("customer_identifier", type=CUSTOMER_IDENTIFIER)
@click.option(
    "--add",
    "names_to_add",
    multiple=True,
    help="Identifier name to allow (repeatable)",
)
@click.option(
    "--remove",
    "names_to_remove",
    multiple=True,
    help="Identifier name to stop allowing (repeatable)",
)
@_table_option
@click.pass_obj
@_handle_errors
def update(
    ctx: AppContext,
    customer_identifier: str,
    names_to_add: tuple[str, ...],
    names_to_remove: tuple[str, ...],
    table_name_substring: str,
) -> None:
    """Add and/or remove allowed identifier names on an existing policy."""
    if not names_to_add and not names_to_remove:
        raise click.UsageError("Specify at least one --add or --remove.")
    service = IdentifierPolicyService(ctx.session, table_name_substring)
    policy = service.update_policy(customer_identifier, names_to_add, names_to_remove)
    click.echo(
        f"UPDATED identifier policy for customer {customer_identifier}: "
        f"{_format_names(policy)}"
    )
    if not policy.allowed_identifier_names:
        logger.warning(
            "The policy no longer allows any identifier names, so the customer is denied all identifiers"
        )


@policies.command()
@click.argument("customer_identifier", type=CUSTOMER_IDENTIFIER)
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt")
@_table_option
@click.pass_obj
@_handle_errors
def delete(
    ctx: AppContext, customer_identifier: str, yes: bool, table_name_substring: str
) -> None:
    """Delete the identifier policy for a customer."""
    service = IdentifierPolicyService(ctx.session, table_name_substring)
    existing_policy = service.get_policy(customer_identifier)
    if existing_policy is None:
        raise click.ClickException(
            f"No identifier policy found for customer {customer_identifier}"
        )
    if not yes:
        click.confirm(
            f"Delete identifier policy for customer {customer_identifier} "
            f"(allowed: {_format_names(existing_policy)})?",
            abort=True,
        )
    service.delete_policy(customer_identifier)
    click.echo(f"DELETED identifier policy for customer {customer_identifier}")


def _format_names(policy: IdentifierPolicy) -> str:
    if not policy.allowed_identifier_names:
        return DENY_ALL_LABEL
    return ", ".join(policy.allowed_identifier_names)


def _customer_names(ctx: AppContext) -> dict[str, str]:
    try:
        return build_customer_lookup(ctx.session)
    except (ValueError, ClientError, BotoCoreError) as error:
        logger.warning(f"Could not resolve customer names: {error}")
        return {}


def _render_policies(
    identifier_policies: list[IdentifierPolicy],
    customer_names: dict[str, str],
    table_name: str,
) -> None:
    console = Console()
    if not identifier_policies:
        console.print(f"[yellow]No identifier policies found in {table_name}[/yellow]")
        return
    table = Table(
        title=f"Identifier policies ({table_name})",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Customer", style="cyan")
    table.add_column("Customer identifier", no_wrap=True)
    table.add_column("Allowed identifier names")
    for policy in identifier_policies:
        table.add_row(
            customer_names.get(policy.customer_identifier, UNKNOWN_CUSTOMER_NAME),
            policy.customer_identifier,
            _format_names(policy),
        )
    console.print(table)
    console.print(f"[dim]Total: {len(identifier_policies)} policy(ies)[/dim]")
