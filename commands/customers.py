import click

from commands.services.aws_utils import prettify
from commands.services.customers_api import (
    list_duplicate_customers,
    list_missing_customers,
)
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
