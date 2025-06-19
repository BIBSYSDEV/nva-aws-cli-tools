import click
import subprocess
import json
import os
from deepdiff import DeepDiff

from commands.services.publication_api import PublicationApiService
from commands.services.aws_utils import prettify
from commands.services.dynamodb_export import DynamodbExport
from boto3.dynamodb.conditions import Attr


@click.group()
def publications():
    pass


@publications.command(
    help="Copy publication, clear assosiated artifacts and set to draft status"
)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.argument("publication_identifier", required=True, nargs=1)
def copy(profile: str, publication_identifier: str) -> None:
    service = PublicationApiService(profile)
    original = service.fetch_publication(publication_identifier)
    original["associatedArtifacts"] = []
    original.pop("identifier")
    original.pop("id")
    original.pop("@context")
    new = service.create_publication(original)
    click.echo(prettify(new))

@publications.command(
    help="Edit a publication by opening it in the chosen editor, e.g., VS Code and saving changes"
)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.option(
    "--editor",
    default="code",
    help="The editor to use for opening the publication, defaults to Visual Studio Code (use 'code')",
)
@click.argument("publication_identifier", required=True, nargs=1)
def edit(profile: str, editor: str, publication_identifier: str) -> None:
    service = PublicationApiService(profile)
    publication = service.fetch_publication(publication_identifier)
    publication.pop("@context", None)

    file_name = f"{publication_identifier}.json"
    
    with open(file_name, "w") as file:
        file.write(prettify(publication))

    try:
        if editor == "code":
            subprocess.run([editor, "--new-window", "--wait", file_name])
        else:
            click.echo(f"Error: The specified editor '{editor}' could not be found.")
            return
    except FileNotFoundError:
        click.echo(f"Error: The specified editor '{editor}' could not be found.")
        return


    with open(file_name, "r") as file:
        updated_publication = json.load(file)

    diff = DeepDiff(publication, updated_publication, ignore_order=True)

    if diff:
        click.echo("Changes detected in the publication:")
        click.echo(diff.pretty())

        if click.confirm("Do you want to save these changes?", default=False):
            service.update_publication(publication_identifier, updated_publication)
            click.echo("Changes saved successfully.")
        else:
            click.echo("Changes were not saved.")
    else:
        click.echo("No changes detected. Nothing to save.")

@publications.command(
    help="Fetch a publication and save it to the current working directory as a JSON file."
)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.argument("publication_identifier", required=True, nargs=1)
def fetch(profile: str, publication_identifier: str) -> None:
    service = PublicationApiService(profile)
    publication = service.fetch_publication(publication_identifier)

    publication.pop("@context", None)

    file_name = f"{publication_identifier}.json"
    with open(file_name, "w") as file:
        json.dump(publication, file, indent=4)

    click.echo(f"Publication saved to {os.path.abspath(file_name)}")


@publications.command(help="Export all publications")
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.option("--folder", required=True, help="The folder to save the exported data.")
def export(profile: str, folder: str) -> None:
    table_pattern = "^nva-resources-master-pipelines-NvaPublicationApiPipeline-.*-nva-publication-api$"
    condition = Attr("PK0").begins_with("Resource:") & Attr("SK0").begins_with(
        "Resource:"
    )
    batch_size = 700
    DynamodbExport(profile, table_pattern, condition, batch_size).save_to_folder(folder)
