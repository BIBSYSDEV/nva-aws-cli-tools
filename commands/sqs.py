import click
from rich.console import Console
from rich.prompt import Confirm

from commands.services.sqs import SqsService

console = Console()


@click.group()
def sqs():
    """Manage SQS queues and messages."""
    pass


@sqs.command()
@click.argument('queue_name', type=str)
@click.option('--profile', type=str, help='AWS profile to use')
@click.option('--output-dir', type=str, help='Output directory for JSONL files')
@click.option('--messages-per-file', type=int, default=1000, help='Max messages per JSONL file (default: 1000)')
@click.option('--delete', is_flag=True, help='Delete messages after writing to file (use with caution)')
@click.option('--threads', type=int, default=5, help='Number of threads for parallel processing (default: 5)')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def drain(queue_name, profile, output_dir, messages_per_file, delete, threads, yes):
    sqs_service = SqsService(profile=profile)

    queue_url = sqs_service.find_queue_url(queue_name)
    if not queue_url:
        return

    queue_full_name = queue_url.split('/')[-1]
    delete_after_write = delete

    if not yes:
        console.print(f"\n[yellow]Queue: {queue_full_name}[/yellow]")
        console.print(f"[yellow]Profile: {sqs_service.profile}[/yellow]")
        console.print(f"[yellow]Messages per file: {messages_per_file}[/yellow]")
        console.print(f"[yellow]Delete after write: {delete_after_write}[/yellow]")
        console.print(f"[yellow]Threads: {threads}[/yellow]")

        if delete_after_write:
            console.print("\n[bold red]WARNING: Messages will be DELETED from the queue after writing![/bold red]")

        if not Confirm.ask("\n[cyan]Proceed with draining the queue?[/cyan]"):
            console.print("[red]Operation cancelled[/red]")
            return

    success = sqs_service.drain_queue(
        queue_name,
        output_dir=output_dir,
        max_messages_per_file=messages_per_file,
        delete_after_write=delete_after_write,
        num_threads=threads
    )

    if not success:
        console.print("[red]Failed to drain queue[/red]")
        raise click.Abort()


@sqs.command()
@click.argument('queue_name', type=str)
@click.option('--profile', type=str, help='AWS profile to use')
def info(queue_name, profile):
    sqs_service = SqsService(profile=profile)

    queue_url = sqs_service.find_queue_url(queue_name)
    if not queue_url:
        return

    queue_full_name = queue_url.split('/')[-1]
    attrs = sqs_service.get_queue_attributes(queue_url)

    if not attrs:
        console.print("[red]Failed to get queue attributes[/red]")
        return

    console.print(f"\n[bold cyan]Queue: {queue_full_name}[/bold cyan]")
    console.print(f"[cyan]URL: {queue_url}[/cyan]")
    console.print(f"[cyan]Profile: {sqs_service.profile}[/cyan]\n")

    console.print("[bold]Message Statistics:[/bold]")
    console.print(f"  Approximate messages: {attrs.get('ApproximateNumberOfMessages', 0)}")
    console.print(f"  Messages in flight: {attrs.get('ApproximateNumberOfMessagesNotVisible', 0)}")
    console.print(f"  Delayed messages: {attrs.get('ApproximateNumberOfMessagesDelayed', 0)}")

    console.print("\n[bold]Queue Configuration:[/bold]")
    console.print(f"  Visibility timeout: {attrs.get('VisibilityTimeout', 'N/A')} seconds")
    console.print(f"  Message retention: {attrs.get('MessageRetentionPeriod', 'N/A')} seconds")
    console.print(f"  Max message size: {attrs.get('MaximumMessageSize', 'N/A')} bytes")
    console.print(f"  Receive wait time: {attrs.get('ReceiveMessageWaitTimeSeconds', 'N/A')} seconds")

    if attrs.get('RedrivePolicy'):
        import json
        redrive = json.loads(attrs['RedrivePolicy'])
        console.print("\n[bold]Dead Letter Queue:[/bold]")
        console.print(f"  Max receive count: {redrive.get('maxReceiveCount', 'N/A')}")
        console.print(f"  DLQ ARN: {redrive.get('deadLetterTargetArn', 'N/A')}")

    console.print(f"\n[dim]Created: {attrs.get('CreatedTimestamp', 'N/A')}[/dim]")
    console.print(f"[dim]Last modified: {attrs.get('LastModifiedTimestamp', 'N/A')}[/dim]")


@sqs.command()
@click.argument('folder_path', type=str)
@click.option('--profile', type=str, help='AWS profile to use')
def analyze(folder_path, profile):
    """Analyze messages from drained SQS queue JSONL files.

    This command analyzes the JSONL files created by the drain command to find:
    - Exception types and error patterns
    - Common message types
    - Longest matching strings in errors
    - Stack trace locations
    - Message and attribute statistics
    """
    sqs_service = SqsService(profile=profile)
    results = sqs_service.analyze_drained_messages(folder_path)

    if not results:
        console.print("[red]No analysis results[/red]")
        raise click.Abort()


@sqs.command()
@click.option('--profile', type=str, help='AWS profile to use')
@click.option('--filter', type=str, help='Filter queues by name pattern')
def list(profile, filter):
    """List all SQS queues in the account."""
    sqs_service = SqsService(profile=profile)

    try:
        response = sqs_service.sqs_client.list_queues()
        queue_urls = response.get('QueueUrls', [])

        if not queue_urls:
            console.print("[yellow]No queues found[/yellow]")
            return

        if filter:
            queue_urls = [url for url in queue_urls if filter.lower() in url.lower()]

        console.print(f"\n[bold cyan]SQS Queues ({sqs_service.profile} profile):[/bold cyan]\n")

        for url in sorted(queue_urls):
            queue_name = url.split('/')[-1]
            console.print(f"  • {queue_name}")

        console.print(f"\n[dim]Total: {len(queue_urls)} queue(s)[/dim]")

    except Exception as e:
        console.print(f"[red]Error listing queues: {e}[/red]")
        raise click.Abort()