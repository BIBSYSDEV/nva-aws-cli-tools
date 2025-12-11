import click

from commands.utils import AppContext
from commands.services.cognito_api import CognitoService
from commands.services.aws_utils import prettify


@click.group()
@click.pass_obj
def cognito(ctx: AppContext):
    pass


@cognito.command(help="Search users by user attribute values")
@click.argument("search_term", required=True, nargs=-1)
@click.pass_obj
def search(ctx: AppContext, search_term: str) -> None:
    search_term = " ".join(search_term)
    result = CognitoService(ctx.profile).search(search_term)
    click.echo(prettify(result))
