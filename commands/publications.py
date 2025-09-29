import click
import csv

from commands.services.publication_api import PublicationApiService
from commands.services.aws_utils import (
    extract_publication_identifier,
    prettify,
    edit_and_diff,
)
from commands.services.dynamodb_publications import DynamodbPublications
from commands.services.resource_batch_job import ResourceBatchJobService
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


@publications.command(help="Fetch a publication")
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

    if not publication:
        click.echo(f"Publication with identifier {publication_identifier} not found.")
        return

    publication.pop("@context", None)

    click.echo(prettify(publication))


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
    _, _, resource = DynamodbPublications(
        profile, table_pattern
    ).fetch_resource_by_identifier(publication_identifier)
    click.echo(prettify(resource))


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
        service.update_resource(
            pk0, sk0, data=service.deflate_resource(updated_publication)
        )

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
    service = DynamodbPublications(profile, table_pattern)

    update_statements = []
    batch_size = 15

    def execute_batch():
        if update_statements:
            print(f"⬆️ Executing batch of {len(update_statements)} updates.")
            service.execute_batch_updates(update_statements)
            update_statements.clear()

    with open(input, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            try:
                publication_identifier = extract_publication_identifier(row["id"])
                new_cristin_id = row["cristinIdentifier"]

                print(
                    f"Processing publication: {publication_identifier} with new Cristin ID: {new_cristin_id}"
                )

                pk0, sk0, resource = service.fetch_resource_by_identifier(
                    publication_identifier
                )

                if not resource:
                    click.echo(
                        f"Publication {publication_identifier} not found in DynamoDB.",
                        err=True,
                    )
                    continue

                new_id_object = {
                    "type": "CristinIdentifier",
                    "value": new_cristin_id,
                    "sourceName": "cristin@nva",
                }
                if new_id_object not in resource.get("additionalIdentifiers", []):
                    resource.setdefault("additionalIdentifiers", []).append(
                        new_id_object
                    )
                    resource["cristinIdentifier"] = new_id_object

                    update_statement = service.prepare_update_resource(
                        pk0,
                        sk0,
                        data=service.deflate_resource(resource),
                        PK4=f"CristinIdentifier:{new_cristin_id}",
                    )
                    update_statements.append(update_statement)

                    click.echo(
                        f"🟢 prepared update for publication: {publication_identifier} with Cristin ID: {new_cristin_id}"
                    )
                else:
                    print(
                        f"🔵 Identifier already exists: {publication_identifier} with Cristin ID: {new_cristin_id}"
                    )

                if len(update_statements) >= batch_size:
                    execute_batch()

            except KeyError as e:
                click.echo(f"Missing expected column in CSV: {e}", err=True)
            except Exception as e:
                click.echo(f"Failed to process publication {row}: {e}", err=True)

    # Execute any remaining updates in the batch
    execute_batch()


@publications.command(
    help="Reindex publications by sending their IDs to SQS queue in batches. Takes a text file with publication IDs."
)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.option(
    "--batch-size",
    default=10,
    help="Number of messages to send per batch (default: 10)",
)
@click.argument("input_file", type=click.Path(exists=True), required=True)
def reindex(profile: str, batch_size: int, input_file: str) -> None:
    """
    Read publication IDs from a text file and send reindex messages to SQS queue.

    The input file should contain one publication identifier per line, e.g.:
    0198cc59d6e8-ca6c9264-31f3-4ab6-b5a5-6494e1ae0b12
    0198cc59dd10-5a7163aa-3dbd-4bcd-b8eb-1898559f5717
    """
    # Initialize the batch job service
    service = ResourceBatchJobService(profile)
    
    click.echo(f"📚 Reading publication IDs from {input_file}")
    click.echo(f"📦 Batch size: {batch_size}")

    # Count the IDs for progress display
    with open(input_file, "r") as f:
        total_ids = sum(1 for line in f if line.strip())

    click.echo(f"📊 Found {total_ids} publication IDs to process")
    click.echo("🚀 Processing batch job...")
    
    # Define progress callback for batch feedback
    def report_batch_progress(batch_successful, batch_size, total_sent, total_ids):
        click.echo(
            f"✅ Sent batch: {batch_successful}/{batch_size} messages "
            f"(Total progress: {total_sent}/{total_ids})"
        )

    # Process the batch job with progress feedback
    result = service.process_reindex_job(input_file, batch_size, report_batch_progress)
    
    # Check if there was an error finding the queue
    if not result["success"] and result.get("error"):
        click.echo(f"❌ {result['error']}", err=True)
        return

    # Display any failures
    if result.get("failures"):
        for failure in result["failures"]:
            click.echo(
                f"❌ Failed to send message: {failure.get('Message', 'Unknown error')}",
                err=True,
            )

    # Final summary
    click.echo("\n📈 Reindexing Summary:")
    click.echo(f"   Total IDs processed: {result['total_processed']}")
    click.echo(f"   Successfully queued: {result['successful']}")
    click.echo(f"   Failed: {result['failed']}")

    if result["success"]:
        click.echo("✨ All publications successfully queued for reindexing!")
    elif result["failed"] > 0:
        click.echo(
            f"⚠️  {result['failed']} messages failed to send. Check the errors above."
        )
