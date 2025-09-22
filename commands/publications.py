import click
import csv
import json
import time
import boto3

from commands.services.publication_api import PublicationApiService
from commands.services.aws_utils import (
    extract_publication_identifier,
    prettify,
    edit_and_diff,
)
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
    session = boto3.Session(profile_name=profile)
    sqs = session.client("sqs")

    # Find the DynamodbResourceBatchJobWorkQueue
    click.echo("🔍 Looking for DynamodbResourceBatchJobWorkQueue...")

    try:
        response = sqs.list_queues()
        all_queues = response.get("QueueUrls", [])

        # Filter queues that match the pattern
        matching_queues = [
            q for q in all_queues if "DynamodbResourceBatchJobWorkQueue" in q
        ]

        if not matching_queues:
            click.echo(
                "❌ No queue found matching pattern *DynamodbResourceBatchJobWorkQueue*",
                err=True,
            )
            return

        if len(matching_queues) > 1:
            click.echo(
                f"⚠️  Found {len(matching_queues)} matching queues. Using the first one:",
                err=True,
            )
            for q in matching_queues:
                click.echo(f"  - {q}")

        queue_url = matching_queues[0]
        click.echo(f"✅ Found queue: {queue_url}")

    except Exception as e:
        click.echo(f"❌ Error finding queue: {str(e)}", err=True)
        return

    total_sent = 0
    failed_count = 0
    batch_messages = []

    click.echo(f"📚 Reading publication IDs from {input_file}")
    click.echo(f"📦 Batch size: {batch_size}")

    with open(input_file, "r") as f:
        lines = [line.strip() for line in f if line.strip()]

    total_ids = len(lines)
    click.echo(f"📊 Found {total_ids} publication IDs to process")

    for i, publication_id in enumerate(lines, 1):
        # Create the SQS message for reindexing
        message_body = {
            "dynamoDbKey": {
                "partitionKey": f"Resource:{publication_id}",
                "sortKey": f"Resource:{publication_id}",
                "indexName": "ResourcesByIdentifier",
            },
            "jobType": "REINDEX_RECORD",
            "parameters": {},
        }

        batch_messages.append(
            {"Id": str(len(batch_messages)), "MessageBody": json.dumps(message_body)}
        )

        # Send batch when it reaches the specified size or at the end
        if len(batch_messages) >= batch_size or i == total_ids:
            try:
                response = sqs.send_message_batch(
                    QueueUrl=queue_url, Entries=batch_messages
                )

                # Check for successful sends
                successful = len(response.get("Successful", []))
                failed = response.get("Failed", [])

                total_sent += successful
                failed_count += len(failed)

                if failed:
                    for failure in failed:
                        click.echo(
                            f"❌ Failed to send message {failure['Id']}: {failure.get('Message', 'Unknown error')}",
                            err=True,
                        )

                click.echo(
                    f"✅ Sent batch: {successful}/{len(batch_messages)} messages "
                    f"(Total progress: {total_sent}/{total_ids})"
                )

                # Clear the batch for next iteration
                batch_messages = []

                # Small delay to avoid throttling
                if i < total_ids:
                    time.sleep(0.1)

            except Exception as e:
                click.echo(f"❌ Error sending batch: {str(e)}", err=True)
                failed_count += len(batch_messages)
                batch_messages = []

    # Final summary
    click.echo("\n📈 Reindexing Summary:")
    click.echo(f"   Total IDs processed: {total_ids}")
    click.echo(f"   Successfully queued: {total_sent}")
    click.echo(f"   Failed: {failed_count}")

    if total_sent == total_ids:
        click.echo("✨ All publications successfully queued for reindexing!")
    elif failed_count > 0:
        click.echo(
            f"⚠️  {failed_count} messages failed to send. Check the errors above."
        )
