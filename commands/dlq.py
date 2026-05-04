import click
import logging
import json
from commands.utils import AppContext
from commands.services.dlq import (
    get_messages,
    summarize_messages,
    delete_messages_with_prefix,
)

logger = logging.getLogger(__name__)


@click.group()
@click.pass_obj
def dlq(
    ctx: AppContext,
):
    """Utility methods for working with SQS dead-letter queues (DLQ)."""
    pass


@dlq.command(
    help="Read all messages in a queue and summarize them by sender and body text."
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
@click.pass_obj
def read(ctx: AppContext, queue: str, count: int) -> None:
    sqs_client = ctx.session.client("sqs")
    messages = get_messages(sqs_client, queue, count)
    by_sender, by_body = summarize_messages(messages)
    click.echo(json.dumps({"by_sender": by_sender, "by_body": by_body}, indent=2))


@dlq.command(
    help="Deletes messages from a queue with a body that matches a specific prefix."
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
@click.pass_obj
def purge(ctx: AppContext, queue: str, count: int, prefix: str, dry_run: bool) -> None:
    sqs_client = ctx.session.client("sqs")

    logger.info(f"Target queue: {queue}")
    logger.info(f"Prefix to match: {prefix}")
    logger.info(f"Max messages to delete: {count}")

    if dry_run:
        messages = get_messages(sqs_client, queue, count)
        to_delete = [msg for msg in messages if msg.get("Body", "").startswith(prefix)]
        by_sender, by_body = summarize_messages(to_delete)
        click.echo(json.dumps(
            {"matched_count": len(to_delete), "by_sender": by_sender, "by_body": by_body},
            indent=2,
        ))
        return
    if not click.confirm("Purge messages from this queue?", default=False):
        logger.info("Aborting...")
        return

    # Delete messages with the specified prefix
    deleted_count = delete_messages_with_prefix(sqs_client, queue, prefix, count)
    logger.info(f"Deleted {deleted_count} messages from {queue=}.")
