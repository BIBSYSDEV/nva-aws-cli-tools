import click
from commands.services.aws_utils import get_account_alias
from commands.services.pipelines import (
    get_pipeline_details_for_account,
    PipelineDetails,
)
from rich.console import Console
from rich.table import Table


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
    table.add_column("Build status")
    table.add_column("Built at")
    table.add_column("Deploy status")
    table.add_column("Deployed at")
    table.add_column("Summary")

    # Sort by last deployment
    sorted_pipelines = sorted(
        pipelines,
        key=lambda x: (x.deploy.get_last_change()),
        reverse=True,
    )

    for pipeline in sorted_pipelines:
        if pipeline.repository == "Unknown":
            continue
        table.add_row(
            pipeline.repository,
            pipeline.branch,
            pipeline.build.get_status_text(),
            pipeline.build.get_last_change(),
            pipeline.deploy.get_status_text(),
            pipeline.deploy.get_last_change(),
            pipeline.summary,
        )

    console.print(table)
    console.print("")
