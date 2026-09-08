from dataclasses import asdict

import click
from rich.console import Console
from rich.table import Table

from commands.services.aws_utils import prettify
from commands.services.customers_api import (
    list_duplicate_customers,
    list_missing_customers,
    search_customers,
)
from commands.services.user_models import Customer
from commands.utils import AppContext


@click.group()
@click.pass_obj
def customers(ctx: AppContext):
    pass


@customers.command(
    help="Search customer references from users that does not exist in the customer table"
)
@click.pass_obj
def list_missing(ctx: AppContext) -> None:
    result = list_missing_customers(ctx.session)
    click.echo(prettify(result))


@customers.command(help="Search dubplicate customer references (same cristin id)")
@click.pass_obj
def list_duplicate(ctx: AppContext) -> None:
    result = list_duplicate_customers(ctx.session)
    click.echo(prettify(result))


@customers.command(
    help="Search customers by name, display name, short name, Cristin id, Feide domain, cname or identifier. Case-insensitive; every word must match."
)
@click.argument("search_term", required=True, nargs=-1)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Print JSON instead of a table",
)
@click.pass_obj
def search(ctx: AppContext, search_term: tuple[str, ...], as_json: bool) -> None:
    joined_search_term = " ".join(search_term)
    matching_customers = search_customers(ctx.session, joined_search_term)
    if as_json:
        click.echo(prettify([asdict(customer) for customer in matching_customers]))
        return
    _render_customers(matching_customers, joined_search_term)


def _render_customers(matching_customers: list[Customer], search_term: str) -> None:
    console = Console()
    if not matching_customers:
        console.print(f"[yellow]No customers matching {search_term!r}[/yellow]")
        return
    table = Table(
        title=f"Customers matching {search_term!r}",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Name", style="cyan")
    table.add_column("Identifier", no_wrap=True)
    table.add_column("Cristin ID")
    table.add_column("Feide domain")
    for customer in matching_customers:
        table.add_row(
            customer.display_name or customer.name,
            customer.identifier,
            _last_path_segment(customer.cristin_id),
            customer.feideOrganizationDomain or "",
        )
    console.print(table)
    console.print(f"[dim]Total: {len(matching_customers)} customer(s)[/dim]")


def _last_path_segment(uri: str | None) -> str:
    if not uri:
        return ""
    return uri.rstrip("/").rsplit("/", 1)[-1]
