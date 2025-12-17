import click
import csv
import json
import logging
from datetime import datetime, timezone

from commands.utils import AppContext
from commands.services.publication_api import PublicationApiService
from commands.services.aws_utils import (
    extract_publication_identifier,
    prettify,
    edit_and_diff,
)
from commands.services.dynamodb_publications import DynamodbPublications
from commands.services.resource_batch_job import ResourceBatchJobService
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger(__name__)

table_pattern = (
    "^nva-resources-master-pipelines-NvaPublicationApiPipeline-.*-nva-publication-api$"
)


@click.group()
@click.pass_obj
def publications(ctx: AppContext):
    pass


@publications.command(
    help="Copy publication, clear assosiated artifacts and set to draft status"
)
@click.argument("publication_identifier", required=True, nargs=1)
@click.pass_obj
def copy(ctx: AppContext, publication_identifier: str) -> None:
    service = PublicationApiService(ctx.profile)
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
    "--editor",
    default="code",
    help="The editor to use for opening the publication, defaults to Visual Studio Code (use 'code')",
)
@click.argument("publication_identifier", required=True, nargs=1)
@click.pass_obj
def edit(ctx: AppContext, editor: str, publication_identifier: str) -> None:
    service = PublicationApiService(ctx.profile)
    publication = service.fetch_publication(publication_identifier)
    publication.pop("@context", None)

    def update_callback(updated_publication):
        service.update_publication(publication_identifier, updated_publication)

    edit_and_diff(publication, update_callback)


@publications.command(help="Fetch a publication")
@click.argument("publication_identifier", required=True, nargs=1)
@click.pass_obj
def fetch(ctx: AppContext, publication_identifier: str) -> None:
    service = PublicationApiService(ctx.profile)
    publication = service.fetch_publication(publication_identifier)

    if not publication:
        click.echo(f"Publication with identifier {publication_identifier} not found.")
        return

    publication.pop("@context", None)

    click.echo(prettify(publication))


@publications.command(help="Export all publications")
@click.option("--folder", required=True, help="The folder to save the exported data.")
@click.pass_obj
def export(ctx: AppContext, folder: str) -> None:
    condition = Attr("PK0").begins_with("Resource:") & Attr("SK0").begins_with(
        "Resource:"
    )
    batch_size = 700
    DynamodbPublications(ctx.profile, table_pattern).save_to_folder(
        condition, batch_size, folder
    )


@publications.command(help="Fetch single publication from DynamoDB")
@click.argument("publication_identifier", required=True, nargs=1)
@click.pass_obj
def fetch_dynamodb(ctx: AppContext, publication_identifier: str) -> None:
    _, _, resource = DynamodbPublications(
        ctx.profile, table_pattern
    ).fetch_resource_by_identifier(publication_identifier)
    click.echo(prettify(resource))


@publications.command(help="Update publication in DynamoDB")
@click.argument("publication_identifier", required=True, nargs=1)
@click.pass_obj
def edit_dynamodb(ctx: AppContext, publication_identifier: str) -> None:
    service = DynamodbPublications(ctx.profile, table_pattern)
    pk0, sk0, resource = service.fetch_resource_by_identifier(publication_identifier)

    def update_callback(updated_publication):
        service.update_resource(
            pk0, sk0, data=service.deflate_resource(updated_publication)
        )

    edit_and_diff(resource, update_callback)


@publications.command(
    help="Migrate Cristin IDs in DynamoDB. This add correct Cristin IDs provided in the CSV file."
)
@click.argument("input", type=click.Path(exists=True), required=True, nargs=1)
@click.pass_obj
def migrate_by_dynamodb(ctx: AppContext, input: str) -> None:
    service = DynamodbPublications(ctx.profile, table_pattern)

    update_statements = []
    batch_size = 15

    def execute_batch():
        if update_statements:
            logger.info(f"⬆️ Executing batch of {len(update_statements)} updates.")
            service.execute_batch_updates(update_statements)
            update_statements.clear()

    with open(input, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            try:
                publication_identifier = extract_publication_identifier(row["id"])
                new_cristin_id = row["cristinIdentifier"]

                logger.info(
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
                    logger.info(
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
    "--batch-size",
    default=10,
    help="Number of messages to send per batch (default: 10)",
)
@click.option(
    "--concurrency",
    default=3,
    help="Number of concurrent batch senders (default: 3)",
)
@click.argument("input_source", required=True)
@click.pass_obj
def reindex(
    ctx: AppContext, batch_size: int, concurrency: int, input_source: str
) -> None:
    """
    Send reindex messages to SQS queue for publication IDs.

    INPUT_SOURCE can be either:
    - A file path containing one publication ID per line
    - A single publication ID (e.g., 0198cc59d6e8-ca6c9264-31f3-4ab6-b5a5-6494e1ae0b12)

    Examples:
        # Reindex from file
        cli.py publications reindex publication_ids.txt

        # Reindex single publication
        cli.py publications reindex 0198cc59d6e8-ca6c9264-31f3-4ab6-b5a5-6494e1ae0b12
    """
    import os

    # Initialize the batch job service
    service = ResourceBatchJobService(ctx.profile)

    # Display input information based on type
    if os.path.isfile(input_source):
        click.echo(f"📚 Processing publication IDs from file: {input_source}")
        # Count IDs for display
        with open(input_source, "r") as f:
            total_ids = sum(1 for line in f if line.strip())
        click.echo(f"📊 Found {total_ids} publication IDs to process")
    else:
        click.echo(f"📄 Processing single publication ID: {input_source}")

    click.echo(f"📦 Batch size: {batch_size}")
    click.echo(f"⚡ Concurrency: {concurrency} concurrent senders")
    click.echo("🚀 Processing batch job...")

    # Define progress callback for batch feedback
    def report_batch_progress(batch_successful, batch_size, total_sent, total_ids):
        click.echo(
            f"✅ Sent batch: {batch_successful}/{batch_size} messages "
            f"(Total progress: {total_sent}/{total_ids})"
        )

    # Process the reindex job (service handles both file and single ID)
    result = service.process_reindex_job(
        input_source, batch_size, report_batch_progress, concurrency
    )

    # Check if there was an error
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


@publications.command(help="Export log entries for a publication to a JSON file")
@click.argument("publication_identifier", required=True, nargs=1)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    help="Output file path (default: {identifier}.json in current directory)",
)
@click.pass_obj
def logs(ctx: AppContext, publication_identifier: str, output: str | None) -> None:
    """
    Export log entries for a publication to a JSON file.

    PUBLICATION_IDENTIFIER is the unique identifier for the publication
    (e.g., 019aa050798d-54f5e9a6-2f77-47f3-b59a-0c78d60728db).

    Examples:
        # Export logs with default filename
        cli.py publications logs 019aa050798d-54f5e9a6-2f77-47f3-b59a-0c78d60728db

        # Export logs to specific file
        cli.py publications logs 019aa050798d-54f5e9a6-2f77-47f3-b59a-0c78d60728db --output /tmp/logs.json
    """
    service = DynamodbPublications(ctx.profile, table_pattern)

    logger.info(f"Fetching log entries for publication: {publication_identifier}")
    log_entries = service.fetch_log_entries(publication_identifier)

    if len(log_entries) == 0:
        logger.warning(f"No log entries found for publication {publication_identifier}")
        return

    output_path = output if output else f"{publication_identifier}.json"
    export_result = {
        "identifier": publication_identifier,
        "exportedAt": datetime.now(timezone.utc).isoformat(),
        "entryCount": len(log_entries),
        "logEntries": log_entries,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(export_result, f, indent=2, ensure_ascii=False)
    logger.info(f"Exported {len(log_entries)} log entries to {output_path}")
