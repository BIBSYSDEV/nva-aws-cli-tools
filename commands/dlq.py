import click
import boto3
from rich.pretty import pprint
from commands.services.dlq import (
    get_messages,
    summarize_messages,
    delete_messages_with_prefix,
)


@click.group()
def dlq():
    """Utility methods for working with SQS dead-letter queues (DLQ)."""
    pass


@dlq.command(
    help="Read all messages in a queue and summarize them by sender and body text."
)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g., sikt-nva-sandbox",
)
@click.option(
    "-q",
    "--queue",
    required=True,
    help="Queue name, e.g. 'master-pipelines-NvaNvi-VOMFSCH5SQAC-nva-nvi-IndexDLQ-LHgCzjoCHXHG'",
)
@click.option(
    "-c",
    "--count",
    default=100,
    help="Max number of messages to read",
)
def read(profile: str, queue: str, count: int) -> None:
    session = boto3.Session(profile_name=profile)
    sqs_client = session.client("sqs")
    messages = get_messages(sqs_client, queue, count)
    by_sender, by_type = summarize_messages(messages)
    print("Summary of messages by sender:")
    pprint(by_sender)
    print("Summary of messages by body text:")
    pprint(by_type)


@dlq.command(
    help="Deletes messages from a queue with a body that matches a specific prefix."
)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g., sikt-nva-sandbox",
)
@click.option(
    "-q",
    "--queue",
    required=True,
    help="Queue name, e.g. 'master-pipelines-NvaNvi-VOMFSCH5SQAC-nva-nvi-IndexDLQ-LHgCzjoCHXHG'",
)
@click.option(
    "-p",
    "--prefix",
    required=True,
    help="Body prefix to filter messages by",
)
@click.option(
    "-c",
    "--count",
    default=100,
    help="Max number of messages to read",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be deleted without actually deleting",
)
def purge(profile: str, queue: str, count: int, prefix: str, dry_run: bool) -> None:
    session = boto3.Session(profile_name=profile)
    sqs_client = session.client("sqs")

    print(f"Target queue: {queue}")
    print(f"Prefix to match: {prefix}")
    print(f"Max messages to delete: {count}")

    if dry_run:
        messages = get_messages(sqs_client, queue, count)
        to_delete = [msg for msg in messages if msg.get("Body", "").startswith(prefix)]
        by_sender, by_type = summarize_messages(to_delete)
        print(f"DRY RUN - Found {len(to_delete)} messages to delete:")
        print("Summary by sender:")
        pprint(by_sender)
        print("Summary by content:")
        pprint(by_type)
        return
    if not click.confirm("Purge messages from this queue?", default=False):
        print("Aborting...")
        return

    # Delete messages with the specified prefix
    deleted_count = delete_messages_with_prefix(sqs_client, queue, prefix, count)
    print(f"Deleted {deleted_count} messages from {queue=}.")
