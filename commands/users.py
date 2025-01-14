import click

from commands.services.users_api import UsersAndRolesService
from commands.services.aws_utils import prettify
from commands.services.external_user import ExternalUserService


@click.group()
def users():
    pass


@users.command(help="Search users by user values")
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.argument("search_term", required=True, nargs=-1)
def search(profile: str, search_term: str) -> None:
    search_term = " ".join(search_term)
    result = UsersAndRolesService(profile).search(search_term)
    click.echo(prettify(result))


@users.command(help="Add external API user")
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
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
def create_external(
    profile: str, customer: str, intended_purpose: str, scopes: str
) -> None:
    external_user = ExternalUserService(profile).create(
        customer, intended_purpose, scopes.split(",")
    )
    external_user.save_to_file()
    click.echo(prettify(external_user.client_data))
