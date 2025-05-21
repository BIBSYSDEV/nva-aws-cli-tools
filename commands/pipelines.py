import click
from commands.services.aws_utils import prettify, get_account_alias
from commands.services.pipelines import CodePipelineService
from rich.console import Console
from rich.table import Table
from rich.text import Text
from collections import defaultdict

@click.group()
def pipelines():
    pass

@pipelines.command(help="Check the current Git branch, repository name, and latest status of all CodePipelines")
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g., sikt-nva-sandbox",
)
def branches(profile: str) -> None:
    profiles = profile.split(",")
    all_results = []

    for single_profile in profiles:
        selected_profile = single_profile.strip()
        alias = get_account_alias(selected_profile)
        single_results = CodePipelineService().get_pipeline_details(selected_profile)
        for result in single_results:
            result["account"] = alias
        all_results.extend(single_results)

    display_table(all_results)

def display_table(results):
    console = Console()

    # Group results by account
    grouped_results = defaultdict(list)
    for result in results:
        grouped_results[result["account"]].append(result)

    # Iterate over each account and display grouped branches and statuses
    for account, pipelines in grouped_results.items():
        console.print(f"[bold magenta]Account: {account}[/bold magenta]")

        # Create a table for the current account
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Repository")
        table.add_column("Branch")
        table.add_column("Status")

        for pipeline in pipelines:
            status = pipeline["status"]
            status_text = (
                Text("✔ Succeeded", style="green")
                if status == "Succeeded"
                else Text(status, style="red")
            )

            table.add_row(
                pipeline["repository"],
                pipeline["branch"],
                status_text,
            )

        # Print the table for the current account
        console.print(table)
        console.print("")  # Add a blank line between account groups
