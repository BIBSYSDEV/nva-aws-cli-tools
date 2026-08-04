import json

import click
from botocore.exceptions import BotoCoreError, ClientError
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

from commands.services.sqs import QueueListing, QueueMessageCounts, SqsService
from commands.utils import AppContext

console = Console()


@click.group()
@click.pass_obj
def sqs(ctx: AppContext) -> None:
    """Manage SQS queues and messages."""


@sqs.command()
@click.argument("queue_name", type=str)
@click.option("--output-dir", type=str, help="Output directory for JSONL files")
@click.option(
    "--messages-per-file",
    type=int,
    default=1000,
    help="Max messages per JSONL file (default: 1000)",
)
@click.option(
    "--delete",
    is_flag=True,
    help="Delete messages after writing to file (use with caution)",
)
@click.option(
    "--threads",
    type=int,
    default=5,
    help="Number of threads for parallel processing (default: 5)",
)
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.pass_obj
def drain(
    ctx: AppContext,
    queue_name: str,
    output_dir: str | None,
    messages_per_file: int,
    delete: bool,
    threads: int,
    yes: bool,
) -> None:
    sqs_service = SqsService(session=ctx.session)

    queue_url = get_queue_url(sqs_service, queue_name)
    delete_after_write = delete

    if not yes:
        show_queue_summary(sqs_service, queue_url)
        console.print(f"[yellow]Messages per file: {messages_per_file}[/yellow]")
        console.print(f"[yellow]Delete after write: {delete_after_write}[/yellow]")
        console.print(f"[yellow]Threads: {threads}[/yellow]")

        if delete_after_write:
            console.print(
                "\n[bold red]WARNING: Messages will be DELETED from the queue after writing![/bold red]"
            )

        if not Confirm.ask("\n[cyan]Proceed with draining the queue?[/cyan]"):
            console.print("[red]Operation cancelled[/red]")
            return

    success = sqs_service.drain_queue(
        queue_url,
        output_dir=output_dir,
        max_messages_per_file=messages_per_file,
        delete_after_write=delete_after_write,
        num_threads=threads,
    )

    if not success:
        console.print("[red]Failed to drain queue[/red]")
        raise click.Abort()


@sqs.command()
@click.argument("queue_name", type=str)
@click.pass_obj
def info(ctx: AppContext, queue_name: str) -> None:
    sqs_service = SqsService(session=ctx.session)

    queue_url = get_queue_url(sqs_service, queue_name)
    show_queue_details(sqs_service, queue_url)


@sqs.command()
@click.argument("folder_path", type=str)
@click.pass_obj
def analyze(ctx: AppContext, folder_path: str) -> None:
    """Analyze messages from drained SQS queue JSONL files.

    This command analyzes the JSONL files created by the drain command to find:
    - Exception types and error patterns
    - Common message types
    - Longest matching strings in errors
    - Stack trace locations
    - Message and attribute statistics
    """
    sqs_service = SqsService(session=ctx.session)
    results = sqs_service.analyze_drained_messages(folder_path)

    if not results:
        console.print("[red]No analysis results[/red]")
        raise click.Abort()


@sqs.command(name="list")
@click.option("--filter", type=str, help="Filter queues by name pattern")
@click.option(
    "--include-empty",
    is_flag=True,
    help="Include queues with no messages available or in flight",
)
@click.pass_obj
def list_queues(ctx: AppContext, filter: str | None, include_empty: bool) -> None:
    """List SQS queues with message counts.

    By default only queues with messages available or in flight are shown.
    Use --include-empty to show all queues.
    """
    sqs_service = SqsService(session=ctx.session)

    try:
        listing = sqs_service.list_queue_message_counts(
            name_filter=filter, include_empty=include_empty
        )
    except (BotoCoreError, ClientError) as e:
        console.print(f"[red]Error listing queues: {e}[/red]")
        raise click.Abort()

    show_queue_listing(listing, sqs_service.profile)


@sqs.command()
@click.argument("queue_name", type=str)
@click.option(
    "--max-messages", "-m", type=int, default=1000, help="Max messages to process"
)
@click.pass_obj
def delete_duplicates(ctx: AppContext, queue_name: str, max_messages: int) -> None:
    sqs_service = SqsService(session=ctx.session)
    queue_url = get_queue_url(sqs_service, queue_name)

    show_queue_summary(sqs_service, queue_url)
    console.print(f"Maximum number of messages to process: {max_messages}")

    if not Confirm.ask("\n[cyan]Delete duplicate messages from the queue?[/cyan]"):
        console.print("[red]Operation cancelled[/red]")
        return

    console.print("[yellow]Deleting duplicate messages from queue...[/yellow]")
    sqs_service.delete_duplicate_messages(queue_url, max_messages)


@sqs.command()
@click.argument("queue_name", type=str)
@click.option(
    "--destination",
    "-d",
    required=True,
    type=str,
    help="Destination queue name to redrive messages to",
)
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.pass_obj
def redrive(ctx: AppContext, queue_name: str, destination: str, yes: bool) -> None:
    """Start a DLQ redrive, moving messages from source to destination queue."""
    sqs_service = SqsService(session=ctx.session)

    source_url = get_queue_url(sqs_service, queue_name)
    destination_url = get_queue_url(sqs_service, destination)

    if not yes:
        show_queue_summary(sqs_service, source_url)
        console.print(
            f"\n[yellow]Destination: {destination_url.split('/')[-1]}[/yellow]"
        )

        if not Confirm.ask("\n[cyan]Start redrive?[/cyan]"):
            console.print("[red]Operation cancelled[/red]")
            return

    sqs_service.start_redrive(source_url, destination_url)
    console.print("[green]Redrive started[/green]")


def show_queue_listing(listing: QueueListing, profile: str) -> None:
    if listing.total_queue_count == 0:
        console.print("[yellow]No queues found[/yellow]")
    elif not listing.queues:
        console.print(
            f"[yellow]No queues with messages ({listing.hidden_empty_count} empty "
            f"queue(s) hidden, use --include-empty to show them)[/yellow]"
        )
    else:
        console.print(build_queue_counts_table(listing.queues, profile))
        console.print(f"[dim]{format_queue_listing_summary(listing)}[/dim]")


def build_queue_counts_table(queues: list[QueueMessageCounts], profile: str) -> Table:
    table = Table(title=f"SQS Queues ({profile} profile)")
    table.add_column("Queue Name", style="cyan", no_wrap=True)
    table.add_column("Messages Available", style="yellow", justify="right")
    table.add_column("Messages In Flight", style="magenta", justify="right")

    for queue in queues:
        table.add_row(
            queue.name,
            str(queue.messages_available),
            str(queue.messages_in_flight),
        )
    return table


def format_queue_listing_summary(listing: QueueListing) -> str:
    summary = f"Total: {len(listing.queues)} queue(s)"
    if listing.hidden_empty_count > 0:
        summary += (
            f" ({listing.hidden_empty_count} empty hidden, use --include-empty to show)"
        )
    return summary


def show_queue_summary(sqs_service: SqsService, queue_url: str) -> None:
    queue_full_name = queue_url.split("/")[-1]
    attrs = sqs_service.get_queue_attributes(queue_url)

    console.print(f"\n[bold cyan]Queue: {queue_full_name}[/bold cyan]")
    console.print(f"[cyan]URL: {queue_url}[/cyan]")
    console.print(f"[cyan]Profile: {sqs_service.profile}[/cyan]")

    console.print("\n[bold]Message Statistics:[/bold]")
    console.print(
        f"  Approximate messages: {attrs.get('ApproximateNumberOfMessages', 0)}"
    )
    console.print(
        f"  Messages in flight: {attrs.get('ApproximateNumberOfMessagesNotVisible', 0)}"
    )
    console.print(
        f"  Delayed messages: {attrs.get('ApproximateNumberOfMessagesDelayed', 0)}"
    )


def show_queue_details(sqs_service: SqsService, queue_url: str) -> None:
    show_queue_summary(sqs_service, queue_url)

    attrs = sqs_service.get_queue_attributes(queue_url)

    console.print("\n[bold]Queue Configuration:[/bold]")
    console.print(
        f"  Visibility timeout: {attrs.get('VisibilityTimeout', 'N/A')} seconds"
    )
    console.print(
        f"  Message retention: {attrs.get('MessageRetentionPeriod', 'N/A')} seconds"
    )
    console.print(f"  Max message size: {attrs.get('MaximumMessageSize', 'N/A')} bytes")
    console.print(
        f"  Receive wait time: {attrs.get('ReceiveMessageWaitTimeSeconds', 'N/A')} seconds"
    )

    if attrs.get("RedrivePolicy"):
        redrive = json.loads(attrs["RedrivePolicy"])
        console.print("\n[bold]Dead Letter Queue:[/bold]")
        console.print(f"  Max receive count: {redrive.get('maxReceiveCount', 'N/A')}")
        console.print(f"  DLQ ARN: {redrive.get('deadLetterTargetArn', 'N/A')}")

    console.print(f"\n[dim]Created: {attrs.get('CreatedTimestamp', 'N/A')}[/dim]")
    console.print(
        f"[dim]Last modified: {attrs.get('LastModifiedTimestamp', 'N/A')}[/dim]"
    )


def get_queue_url(sqs_service: SqsService, queue_name: str) -> str:
    queue_url = sqs_service.find_queue_url(queue_name)
    if not queue_url:
        console.print(f"[red]Queue '{queue_name}' not found[/red]")
        raise click.Abort()
    return queue_url
