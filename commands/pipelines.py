import click
from commands.services.aws_utils import get_account_alias
from commands.services.pipelines import (
    get_pipeline_details_for_account,
    PipelineDetails,
)
from rich.console import Console
from rich.table import Table
from rich.text import Text
from collections import defaultdict


@click.group()
def pipelines():
    pass


@pipelines.command(
    help="Check the current Git branch, repository name, and latest status of all CodePipelines"
)
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g., sikt-nva-sandbox",
)
def branches(profile: str) -> None:
    console = Console()
    profiles = profile.split(",")

    for single_profile in profiles:
        selected_profile = single_profile.strip()
        alias = get_account_alias(selected_profile)
        console.print(
            f"[bold magenta]Fetching pipeline details for account: {alias} ({selected_profile})...[/bold magenta]"
        )
        pipelines = get_pipeline_details_for_account(selected_profile)

        console.print(
            f"[bold magenta]Account: {alias} ({len(pipelines)} pipelines)[/bold magenta]"
        )
        display_table(pipelines, console)


def display_table(pipelines: list[PipelineDetails], console: Console) -> None:
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Repository")
    table.add_column("Branch")
    table.add_column("Status")
    table.add_column("Deployed at")

    sorted_pipelines = sorted(
        pipelines,
        key=lambda x: (x.deploy.deployed_at),
        reverse=True,
    )

    for pipeline in sorted_pipelines:
        deployed_at = (
            pipeline.deploy.deployed_at.strftime("%Y-%m-%d %H:%M:%S")
            if pipeline.deploy.deployed_at
            else "N/A"
        )
        table.add_row(
            pipeline.source.repository,
            pipeline.source.branch,
            pipeline.get_status_text(),
            deployed_at,
        )

    console.print(table)
    console.print("")
