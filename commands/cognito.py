import click

from commands.services.cognito_api import CognitoService
from commands.services.aws_utils import prettify


@click.group()
def cognito():
    pass


@cognito.command(help="Search users by user attribute values")
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.argument("search_term", required=True, nargs=-1)
def search(profile: str, search_term: str) -> None:
    search_term = " ".join(search_term)
    result = CognitoService(profile).search(search_term)
    click.echo(prettify(result))
