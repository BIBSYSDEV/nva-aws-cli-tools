import click
import sys
import json

from commands.services.cristin import CristinService
from commands.services.aws_utils import prettify


@click.group()
def cristin():
    pass

@cristin.command(
    help="Add cristin user by passing user data as a JSON string from a file or stdin."
)
@click.argument("input_file", type=click.File("r"), default=sys.stdin)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
def add_user(profile: str, input_file) -> None:
    """
    Adds a user to Cristin. User data is read from INPUT_FILE (json).
    If INPUT_FILE is not provided, it reads from stdin.
    """
    if input_file.isatty():
        user_data_json = sys.stdin.read()
    else:
        user_data_json = input_file.read()
    user_data = json.loads(user_data_json)
    result = CristinService(profile).add_person(user_data)
    click.echo(prettify(result))


