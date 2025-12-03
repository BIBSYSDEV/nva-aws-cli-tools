import json
import click
import sys

from commands.utils import AppContext
from commands.services.users_api import UsersAndRolesService
from commands.services.aws_utils import prettify
from commands.services.external_user import ExternalUserService


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
@click.pass_obj
def create_external(
    ctx: AppContext, customer: str, intended_purpose: str, scopes: str
) -> None:
    external_user = ExternalUserService(ctx.profile).create(
        customer, intended_purpose, scopes.split(",")
    )
    external_user.save_to_file()
    click.echo(prettify(external_user.client_data))
