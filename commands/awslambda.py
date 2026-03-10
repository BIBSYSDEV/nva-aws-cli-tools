import json
from pathlib import Path

import click
from rich.console import Console
from rich.json import JSON
from rich.prompt import Confirm
from rich.table import Table

from commands.utils import AppContext
from commands.services.lambda_api import LambdaService

console = Console()


@click.group()
@click.pass_obj
def awslambda(ctx: AppContext):
    pass


@awslambda.command()
@click.argument("function_name", type=str)
@click.option("--body", type=str, help="JSON payload to send to the function")
@click.option(
    "--body-file",
    type=click.Path(exists=True),
    help="Path to a JSON file containing the payload",
)
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.pass_obj
def invoke(ctx: AppContext, function_name: str, body: str, body_file: str, yes: bool):
    """Invoke a Lambda function asynchronously."""
    if body and body_file:
        raise click.UsageError("Cannot specify both --body and --body-file")

    lambda_service = LambdaService(ctx.profile)

    resolved_name = resolve_function_name(lambda_service, function_name)

    payload = None
    if body_file:
        payload = Path(body_file).read_text()
    elif body:
        payload = body

    if payload:
        json.loads(payload)  # validate JSON

    if not yes:
        console.print(f"\n[bold cyan]Function: {resolved_name}[/bold cyan]")
        if payload:
            console.print("[yellow]Payload:[/yellow]")
            console.print(JSON(payload))
        else:
            console.print("[yellow]Payload: (none)[/yellow]")

        if not Confirm.ask("\n[cyan]Invoke function?[/cyan]"):
            console.print("[red]Operation cancelled[/red]")
            return

    lambda_service.invoke_function(resolved_name, payload)
    console.print("[green]Function invoked[/green]")


def resolve_function_name(lambda_service: LambdaService, name_partial: str) -> str:
    matches = lambda_service.find_function_name(name_partial)

    if not matches:
        console.print(f"[red]No functions found matching '{name_partial}'[/red]")
        raise click.Abort()

    if len(matches) == 1:
        return matches[0]

    table = Table(title="Multiple functions found")
    table.add_column("Index", style="cyan")
    table.add_column("Function Name", style="yellow")

    for i, name in enumerate(matches):
        table.add_row(str(i + 1), name)

    console.print(table)

    while True:
        choice = click.prompt("Select a function", type=int)
        if 1 <= choice <= len(matches):
            return matches[choice - 1]
        console.print(f"[red]Please enter a number between 1 and {len(matches)}[/red]")


@awslambda.command(help="Delete old versions of AWS Lambda functions.")
@click.option("--delete", is_flag=True, default=True, help="Delete old versions.")
@click.pass_obj
def delete_old_versions(ctx: AppContext, delete: bool) -> None:
    LambdaService(ctx.profile).delete_old_versions(delete)


@awslambda.command(help="Generate concurrency report for AWS Lambda functions.")
@click.pass_obj
def concurrency(ctx: AppContext) -> None:
    LambdaService(ctx.profile).concurrency()
