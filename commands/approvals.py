import json
import logging

import click
from botocore.exceptions import BotoCoreError, ClientError
from deepdiff import DeepDiff
from rich.console import Console
from rich.table import Table

from commands.services.approvals_api import (
    AUTHENTICATION_ATTRIBUTE,
    BASE_URI_ATTRIBUTE,
    CLIENT_SECRET_FIELD,
    CREDENTIALS_FIELDS,
    CREDENTIALS_KEY_ATTRIBUTE,
    CREDENTIALS_SECRET_NAME,
    CUSTOMER_IDENTIFIER_ATTRIBUTE,
    DEFAULT_TABLE_NAME_SUBSTRING,
    EMPTY_POLICY,
    NAMES_ATTRIBUTE,
    NO_AUTHENTICATION,
    OAUTH2_CREDENTIALS_TYPE,
    SCOPE_FIELD,
    SOURCE_CONFIGS_ATTRIBUTE,
    TYPE_ATTRIBUTE,
    ApprovalsService,
    parse_customer_identifier,
    read_credentials,
    required_credentials_keys,
    validated_policy,
    write_credentials,
)
from commands.services.aws_utils import prettify
from commands.services.customers_api import build_customer_lookup
from commands.utils import AppContext

logger = logging.getLogger(__name__)

UNKNOWN_CUSTOMER_NAME = "?"
DENY_ALL_LABEL = "(none - denies all)"
POLICY_TEMPLATE = {
    NAMES_ATTRIBUTE: ["rek"],
    SOURCE_CONFIGS_ATTRIBUTE: [
        {
            BASE_URI_ATTRIBUTE: "https://api.rekportalen.no/",
            AUTHENTICATION_ATTRIBUTE: {TYPE_ATTRIBUTE: NO_AUTHENTICATION},
        }
    ],
}


class CustomerIdentifierType(click.ParamType):
    name = "customer"

    def convert(self, value, param, ctx) -> str:
        if not isinstance(value, str):
            return value
        try:
            return parse_customer_identifier(value)
        except ValueError as error:
            self.fail(str(error), param, ctx)


@click.group()
@click.pass_obj
def approvals(ctx: AppContext) -> None:
    """Manage data owned by nva-handle-service (approvals table)."""


@approvals.command()
@click.argument("customer_identifier", type=CustomerIdentifierType(), required=False)
@click.option(
    "--table",
    "table_name_substring",
    default=DEFAULT_TABLE_NAME_SUBSTRING,
    show_default=True,
    help="Substring of the approvals DynamoDB table name. Make it more specific when several stacks match.",
)
@click.pass_obj
def policy(
    ctx: AppContext, customer_identifier: str, table_name_substring: str
) -> None:
    """List the identifier policies, or edit one customer's policy as JSON in $EDITOR.

    Without CUSTOMER_IDENTIFIER (a customer UUID or customer URI) the policies of all
    customers are listed. With one, its allowedIdentifierNames and sourceConfigs open
    in $EDITOR; everything else in the row is left untouched. OAuth2 sources naming
    an unknown credentials key ask for those credentials and write them too.
    """
    service = ApprovalsService(ctx.session, table_name_substring)
    if not customer_identifier:
        _render_policies(service.list_policies(), ctx, service.table_name)
        return
    existing_item = service.get_policy(customer_identifier)
    current = (
        {
            attribute: existing_item[attribute]
            for attribute in EMPTY_POLICY
            if attribute in existing_item
        }
        if existing_item
        else POLICY_TEMPLATE
    )
    edited = click.edit(prettify(EMPTY_POLICY | current), extension=".json")
    if edited is None:
        click.echo("No changes made.")
        return
    try:
        wanted = validated_policy(json.loads(edited))
        stored_credentials = read_credentials(ctx.session)
    except (ValueError, AttributeError) as error:
        raise click.ClickException(str(error))
    difference = DeepDiff(EMPTY_POLICY | current, wanted)
    if not difference:
        click.echo("No changes made.")
        return
    missing_credentials = {
        credentials_key: _prompt_credentials(credentials_key)
        for credentials_key in required_credentials_keys(wanted)
        if credentials_key not in stored_credentials
    }
    click.echo(difference.pretty())
    click.confirm(
        f"Write this policy for customer {customer_identifier} to {service.table_name} "
        f"(profile {ctx.session.profile_name})?",
        abort=True,
    )
    if missing_credentials:
        write_credentials(ctx.session, missing_credentials)
    service.put_policy(customer_identifier, wanted)
    click.echo(f"WROTE identifier policy for customer {customer_identifier}")


@approvals.command()
@click.argument("credentials_key")
@click.pass_obj
def credentials(ctx: AppContext, credentials_key: str) -> None:
    """Store OAuth2 client credentials under CREDENTIALS_KEY in the ApprovalCredentials secret.

    CREDENTIALS_KEY is the key a source config's authentication refers to, by
    convention the host of its baseUri. Other keys in the secret are kept.
    """
    try:
        action = (
            "Replace" if credentials_key in read_credentials(ctx.session) else "Add"
        )
    except ValueError as error:
        raise click.ClickException(str(error))
    new_credentials = {credentials_key: _prompt_credentials(credentials_key)}
    click.confirm(
        f"{action} the credentials under {credentials_key} in the "
        f"{CREDENTIALS_SECRET_NAME} secret (profile {ctx.session.profile_name})?",
        abort=True,
    )
    write_credentials(ctx.session, new_credentials)
    click.echo(f"WROTE credentials under {credentials_key}")


def _prompt_credentials(credentials_key: str) -> dict[str, str]:
    click.echo(f"OAuth2 credentials for key {credentials_key}:")
    return {TYPE_ATTRIBUTE: OAUTH2_CREDENTIALS_TYPE} | {
        field: click.prompt(
            field,
            hide_input=field == CLIENT_SECRET_FIELD,
            default="" if field == SCOPE_FIELD else None,
            show_default=False,
        )
        for field in CREDENTIALS_FIELDS
    }


def _customer_names(ctx: AppContext) -> dict[str, str]:
    try:
        return build_customer_lookup(ctx.session)
    except (ValueError, ClientError, BotoCoreError) as error:
        logger.warning(f"Could not resolve customer names: {error}")
        return {}


def _render_policies(items: list[dict], ctx: AppContext, table_name: str) -> None:
    console = Console()
    if not items:
        console.print(f"[yellow]No identifier policies found in {table_name}[/yellow]")
        return
    customer_names = _customer_names(ctx)
    table = Table(
        title=f"Identifier policies ({table_name})",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Customer", style="cyan")
    table.add_column("Customer identifier", no_wrap=True)
    table.add_column("Allowed identifier names")
    table.add_column("Sources")
    for item in items:
        customer_identifier = str(item.get(CUSTOMER_IDENTIFIER_ATTRIBUTE, ""))
        table.add_row(
            customer_names.get(customer_identifier, UNKNOWN_CUSTOMER_NAME),
            customer_identifier,
            ", ".join(item.get(NAMES_ATTRIBUTE) or []) or DENY_ALL_LABEL,
            "\n".join(
                _format_source(source)
                for source in item.get(SOURCE_CONFIGS_ATTRIBUTE) or []
            ),
        )
    console.print(table)
    console.print(f"[dim]Total: {len(items)} policy(ies)[/dim]")


def _format_source(source_config: dict) -> str:
    authentication = source_config.get(AUTHENTICATION_ATTRIBUTE) or {}
    label = ": ".join(
        str(part)
        for part in (
            authentication.get(TYPE_ATTRIBUTE),
            authentication.get(CREDENTIALS_KEY_ATTRIBUTE),
        )
        if part
    )
    return f"{source_config.get(BASE_URI_ATTRIBUTE)} ({label})"
