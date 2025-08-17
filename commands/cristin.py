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


@cristin.command(help="Update an existing person in Cristin.")
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

@cristin.command(help="Get person from Cristin.")
@click.argument("user_id", required=True)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
def get_person(profile: str, user_id) -> None:
    result = CristinService(profile).get_person(user_id)
    click.echo(prettify(result))

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
    user_service = UsersAndRolesService(profile)
    cristin_service = CristinService(profile)
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            try:
                with open(file_path, "r", encoding="utf-8") as json_file:
                    user_data = json.load(json_file)
                    # Add the user to Cristin
                    cristin_person = cristin_service.get_person_by_nin(user_data["norwegian_national_id"])
                    if cristin_person:
                        cristin_service.update_person(cristin_person["cristin_person_id"], user_data)
                        click.echo(f"User already exists in Cristin, updating: {cristin_person['cristin_person_id']}")
                    else:
                        cristin_person = cristin_service.add_person(user_data)
                        click.echo(f"User added: {prettify(cristin_person)}")
                    
                    
                    # make sure user also exists in NVA and have accepted terms
                    cristin_person_id = cristin_person.get("cristin_person_id")
                    if cristin_person_id:
                        nva_user = user_service.get_user_by_username(f"{cristin_person_id}@20754.0.0.0")

                        if not nva_user:
                            user_service.add_user({
                                "cristinIdentifier": cristin_person_id,
                                "customerId": "https://api.dev.nva.aws.unit.no/customer/bb3d0c0c-5065-4623-9b98-5810983c2478",
                                "roles": [{ "type": "Role", "rolename": "Creator" }],
                                "viewingScope": { "type": "ViewingScope", "includedUnits": [] }
                            })
                        else:
                            click.echo(f"User already exists in NVA: {nva_user['username']}")

                        nva_user["roles"] = [ {
                            "rolename" : "Editor",
                            "accessRights" : [ "MANAGE_RESOURCES_ALL", "MANAGE_OWN_AFFILIATION" ],
                            "type" : "Role"
                        }, {
                            "rolename" : "Support-Curator",
                            "accessRights" : [ "MANAGE_RESOURCES_STANDARD", "SUPPORT" ],
                            "type" : "Role"
                        }, {
                            "rolename" : "Nvi-Curator",
                            "accessRights" : [ "MANAGE_RESOURCES_STANDARD", "MANAGE_NVI_CANDIDATES" ],
                            "type" : "Role"
                        }, {
                            "rolename" : "Creator",
                            "accessRights" : [ "MANAGE_OWN_RESOURCES" ],
                            "type" : "Role"
                        }, {
                            "rolename" : "Institution-admin",
                            "accessRights" : [ "MANAGE_OWN_AFFILIATION" ],
                            "type" : "Role"
                        }, {
                            "rolename" : "Curator-thesis-embargo",
                            "accessRights" : [ "MANAGE_DEGREE_EMBARGO", "MANAGE_RESOURCES_STANDARD" ],
                            "type" : "Role"
                        }, {
                            "rolename" : "Publishing-Curator",
                            "accessRights" : [ "MANAGE_PUBLISHING_REQUESTS", "MANAGE_RESOURCES_STANDARD", "MANAGE_RESOURCE_FILES" ],
                            "type" : "Role"
                        }, {
                            "rolename" : "Doi-Curator",
                            "accessRights" : [ "MANAGE_DOI", "MANAGE_RESOURCES_STANDARD" ],
                            "type" : "Role"
                        }, {
                            "rolename" : "Curator-thesis",
                            "accessRights" : [ "MANAGE_DEGREE", "MANAGE_RESOURCES_STANDARD" ],
                            "type" : "Role"
                        } ]

                        user_service.update_user(nva_user)
                        click.echo(f"User roles updated in NVA: {nva_user['username']}")

                        user_service.approve_terms(cristin_person_id)
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

@cristin.command(help="Get project from Cristin.")
@click.argument("project_id", required=True)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
def get_project(profile: str, project_id) -> None:
    result = CristinService(profile).get_project(project_id)
    click.echo(prettify(result))

@cristin.command(help="Add project to Cristin.")
@click.argument("input_file", type=click.File("r"), default=sys.stdin)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
def add_project(profile: str, input_file) -> None:
    if input_file.isatty():
        data = sys.stdin.read()
    else:
        data = input_file.read()
    project = json.loads(data)

    result = CristinService(profile).add_project(project)
    click.echo(prettify(result))


@cristin.command(help="Update project in Cristin.")
@click.argument("project_id", required=True)
@click.argument("input_file", type=click.File("r"), default=sys.stdin)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
def update_project(profile: str, project_id: str, input_file) -> None:
    if input_file.isatty():
        data = sys.stdin.read()
    else:
        data = input_file.read()
    project = json.loads(data)

    CristinService(profile).update_project(project_id, project)
    click.echo("Project updated successfully.")


@cristin.command(help="Import projects to Cristin from folder with json files.")
@click.argument("input_folder", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
def import_projects(profile: str, input_folder: str) -> None:
    cristin_service = CristinService(profile)
    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(input_folder, filename)
            with open(file_path, "r") as input_file:
                data = json.load(input_file)
                project = cristin_service.find_project_by_title(data["title"]["nb"])
                if not project:
                    click.echo(f"Project {data['title']['nb']} not found in Cristin, adding...")
                    project = cristin_service.add_project(data)
                    click.echo(project)
                    click.echo(f"Imported project from {file_path}")
                else:
                    click.echo(f"Project {project['cristin_project_id']} ({project['title']['nb']}) already exists in Cristin, updating...")
                    cristin_service.update_project(project["cristin_project_id"], data)
                    click.echo(f"Updated project from file: {file_path}")
    click.echo("All projects imported successfully. When adding, make sure to wait a bit for search indexing before retrying.")
