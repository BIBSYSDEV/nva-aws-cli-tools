import click
import sys
import json
import os

from commands.services.cristin import CristinService
from commands.services.users_api import UsersAndRolesService
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
def add_person(profile: str, input_file) -> None:
    """
    Adds a person to Cristin. Person data is read from INPUT_FILE (json).
    If INPUT_FILE is not provided, it reads from stdin.
    """
    if input_file.isatty():
        user_data_json = sys.stdin.read()
    else:
        user_data_json = input_file.read()
    user_data = json.loads(user_data_json)
    result = CristinService(profile).add_person(user_data)
    click.echo(prettify(result))

@cristin.command(
    help="Update an existing person in Cristin."
)
@click.argument("user_id", required=True)
@click.argument("input_file", type=click.File("r"), default=sys.stdin)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
def update_person(profile: str, input_file, user_id) -> None:
    if input_file.isatty():
        user_data_json = sys.stdin.read()
    else:
        user_data_json = input_file.read()
    user_data = json.loads(user_data_json)
    CristinService(profile).update_person(user_id, user_data)

@cristin.command(
    help="Add cristin persons from all JSON files in a folder and pre-approve their terms."
)
@click.argument(
    "folder_path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True),
)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
def import_persons(profile: str, folder_path: str) -> None:
    """
    Adds users to Cristin from all JSON files in the specified folder and pre-approves their terms.
    """
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            try:
                with open(file_path, "r", encoding="utf-8") as json_file:
                    user_data = json.load(json_file)
                    # Add the user to Cristin
                    add_result = CristinService(profile).add_person(user_data)
                    click.echo(f"User added: {prettify(add_result)}")

                    # Pre-approve terms if the user was added successfully
                    cristin_person_id = add_result.get("cristin_person_id")
                    if cristin_person_id:
                        UsersAndRolesService(profile).approve_terms(cristin_person_id)
                        click.echo(f"Terms pre-approved for user {cristin_person_id}")
                    else:
                        click.echo(
                            f"Failed to retrieve Cristin person ID for user in file: {filename}"
                        )

            except json.JSONDecodeError:
                click.echo(f"Invalid JSON in file: {file_path}")
            except Exception as e:
                click.echo(
                    f"An error occurred while processing file {file_path}: {str(e)}"
                )
