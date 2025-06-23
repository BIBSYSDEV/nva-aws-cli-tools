import click
import json
import os
import csv

from commands.services.publication_api import PublicationApiService
from commands.services.aws_utils import extract_publication_identifier, prettify, edit_and_diff
from commands.services.dynamodb_publications import DynamodbPublications
from boto3.dynamodb.conditions import Attr

table_pattern = (
    "^nva-resources-master-pipelines-NvaPublicationApiPipeline-.*-nva-publication-api$"
)


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

    def update_callback(updated_publication):
        service.update_publication(publication_identifier, updated_publication)

    edit_and_diff(publication, update_callback)


@publications.command(
    help="Fetch a publication and save it to the publication_data folder as a JSON file."
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

    # Define the folder to store publications
    folder_name = "publication_data"
    os.makedirs(folder_name, exist_ok=True)  # Create the folder if it doesn't exist

    file_name = os.path.join(folder_name, f"{publication_identifier}.json")
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
    condition = Attr("PK0").begins_with("Resource:") & Attr("SK0").begins_with(
        "Resource:"
    )
    batch_size = 700
    DynamodbPublications(profile, table_pattern).save_to_folder(
        condition, batch_size, folder
    )


@publications.command(help="Fetch single publication from DynamoDB")
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.argument("publication_identifier", required=True, nargs=1)
def fetch_dynamodb(profile: str, publication_identifier: str) -> None:
    click.echo(
        prettify(
            DynamodbPublications(profile, table_pattern).fetch_resource_by_identifier(
                publication_identifier
            )
        )
    )


@publications.command(help="Update publication in DynamoDB")
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.argument("publication_identifier", required=True, nargs=1)
def edit_dynamodb(profile: str, publication_identifier: str) -> None:
    service = DynamodbPublications(profile, table_pattern)
    pk0, sk0, resource = service.fetch_resource_by_identifier(publication_identifier)

    def update_callback(updated_publication):
        service.update_resource(pk0, sk0, data=service.deflate_resource(updated_publication))

    edit_and_diff(resource, update_callback)


@publications.command(
    help="Migrate Cristin IDs in DynamoDB. This add correct Cristin IDs provided in the CSV file."
)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.argument("input", type=click.Path(exists=True), required=True, nargs=1)
def migrate_by_dynamodb(profile: str, input: str) -> None:
    # Initialize the DynamoDB service
    service = DynamodbPublications(profile, table_pattern)

    update_statements = []
    batch_size = 15  # Batch size for updates

    def execute_batch():
        if update_statements:
            service.execute_batch_updates(update_statements)
            update_statements.clear()

    with open(input, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            try:
                publication_identifier = extract_publication_identifier(row["id"])
                new_cristin_id = row["cristinIdentifier"]

                print(f"Processing publication: {publication_identifier} with new Cristin ID: {new_cristin_id}")

                pk0, sk0, resource = service.fetch_resource_by_identifier(publication_identifier)

                if not resource:
                    click.echo(
                        f"Publication {publication_identifier} not found in DynamoDB.",
                        err=True,
                    )
                    continue

                new_id_object = {
                    "type": "CristinIdentifier",
                    "value": new_cristin_id,
                    "sourceName": "cristin@nibio",
                }
                if new_id_object not in resource.get("additionalIdentifiers", []):
                    resource.setdefault("additionalIdentifiers", []).append(new_id_object)
                else:
                    print("Identifier already exists.")
                resource["cristinIdentifier"] = new_id_object

                # Prepare the update statement
                update_statement = service.prepare_update_resource(
                    pk0, sk0, data=service.deflate_resource(resource), PK4=f"CristinIdentifier:{new_cristin_id}"
                )
                update_statements.append(update_statement)

                # Execute batch updates if batch size is reached
                if len(update_statements) >= batch_size:
                    execute_batch()

                click.echo(
                    f"Successfully prepared update for publication: {publication_identifier} with Cristin ID: {new_cristin_id}"
                )

            except KeyError as e:
                click.echo(f"Missing expected column in CSV: {e}", err=True)
            except Exception as e:
                click.echo(f"Failed to process publication {row}: {e}", err=True)

    # Execute any remaining updates in the batch
    execute_batch()
