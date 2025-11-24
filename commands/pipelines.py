import click
import logging
from commands.services.aws_utils import get_account_alias
from commands.services.pipelines import get_pipeline_details_for_account

from rich.console import Console
from rich.table import Table

logger = logging.getLogger(__name__)


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
    profiles = profile.split(",")
    for single_profile in profiles:
        show_summary_table(single_profile.strip())


def show_summary_table(profile: str) -> None:
    console = Console()
    alias = get_account_alias(profile)
    logger.info(f"Fetching pipeline details for account: {alias} ({profile})...")
    pipelines = get_pipeline_details_for_account(profile)

    # Define the table
    table = Table(
        show_header=True,
        header_style="bold cyan",
        show_lines=True,
        title=f"[bold magenta]Account: {alias} ({len(pipelines)} pipelines)[/bold magenta]",
        caption=f"[bold magenta]{alias}[/bold magenta]",
    )
    table.add_column("Repository")
    table.add_column("Branch")
    table.add_column("Status")
    table.add_column("Last triggered", no_wrap=True, justify="left", max_width=50)
    table.add_column("Last deploy", no_wrap=True, justify="left", max_width=50)

    # Sort by last deployment
    sorted_pipelines = sorted(
        pipelines,
        key=lambda x: (x.last_deploy.get_last_change()),
        reverse=True,
    )

    for pipeline in sorted_pipelines:
        if pipeline.repository == "Unknown":
            continue
        table.add_row(
            pipeline.repository,
            pipeline.branch,
            pipeline.get_status_text(),
            pipeline.get_link_to_last_commit(),
            pipeline.get_link_to_deployed_commit(),
        )

    console.print(table)
    console.print("")
