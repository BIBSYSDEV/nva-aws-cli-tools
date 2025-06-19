import click
import os

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
    help="Fetch a publication and open it in the chosen editor, e.g., VS Code"
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
def fetch(profile: str, editor: str, publication_identifier: str) -> None:
    service = PublicationApiService(profile)
    publication = service.fetch_publication(publication_identifier)

    # Write the content to a local file in the current working directory
    file_name = f"{publication_identifier}.json"
    
    with open(file_name, "w") as file:
        file.write(prettify(publication))

    # Open the file in the specified editor
    os.system(f"{editor} {file_name}")


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
