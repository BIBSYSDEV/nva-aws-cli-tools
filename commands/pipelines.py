import logging

import boto3
import click
from mypy_boto3_codepipeline.client import CodePipelineClient
from rich.console import Console
from rich.table import Table

from commands.services import pipeline_branch
from commands.services.aws_utils import get_account_alias
from commands.services.pipelines import get_pipeline_details_for_account
from commands.utils import AppContext

logger = logging.getLogger(__name__)


@click.group()
@click.pass_obj
def pipelines(ctx: AppContext):
    pass


@pipelines.command(
    help="Check the current Git branch, repository name, and latest status of all CodePipelines"
)
@click.pass_obj
def branches(ctx: AppContext) -> None:
    show_summary_table(ctx.session)


def show_summary_table(session: boto3.Session) -> None:
    console = Console()
    alias = get_account_alias(session)
    logger.info(f"Fetching pipeline details for account: {alias}...")
    pipelines = get_pipeline_details_for_account(session)

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

    sorted_pipelines = sorted(
        pipelines,
        key=lambda pipeline: pipeline.last_deploy.get_last_change(),
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


def with_pipeline_selection(command):
    command = click.option(
        "--pipeline",
        "pipeline_name",
        help="Pipeline name (default: looked up from the repository)",
    )(command)
    return click.option(
        "--repository",
        "-r",
        help="GitHub repository, OWNER/REPO (default: from the git remote origin of the current directory)",
    )(command)


@pipelines.command(
    help="Point this repository's CodePipeline at a Git branch and start an execution. "
    "Run it from inside the service repository."
)
@with_pipeline_selection
@click.option(
    "--branch", "-b", help="Branch to deploy (default: the current Git branch)"
)
@click.option(
    "--no-start",
    "start_execution",
    is_flag=True,
    flag_value=False,
    default=True,
    help="Update the pipeline without starting an execution",
)
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt")
@click.pass_obj
def deploy(
    ctx: AppContext,
    repository: str | None,
    pipeline_name: str | None,
    branch: str | None,
    start_execution: bool,
    yes: bool,
) -> None:
    codepipeline = ctx.session.client("codepipeline")
    pipeline_name = pipeline_name or resolve_pipeline_name(codepipeline, repository)
    pipeline_definition = codepipeline.get_pipeline(name=pipeline_name)["pipeline"]
    source_repository = pipeline_branch.source_repository(pipeline_definition)
    if not source_repository:
        raise click.ClickException(
            f"Pipeline {pipeline_name} has no GitHub (CodeStar connection) source, "
            "so there is no branch to change."
        )

    branch = branch or pipeline_branch.current_git_branch()
    if not branch:
        raise click.UsageError(
            "Could not determine the current branch (detached HEAD?). Use --branch."
        )
    try:
        branch_exists = pipeline_branch.branch_exists_in_repository(
            source_repository, branch
        )
    except pipeline_branch.BranchCheckError as error:
        raise click.ClickException(str(error)) from error
    if not branch_exists:
        raise click.ClickException(
            f"Branch '{branch}' does not exist in {source_repository}. Push it first."
        )

    click.echo(f"Account:    {get_account_alias(ctx.session)}")
    click.echo(f"Pipeline:   {pipeline_name}")
    click.echo(f"Repository: {source_repository}")
    click.echo(
        f"Branch:     {pipeline_branch.configured_branch(pipeline_definition)} -> {branch}"
    )
    if not yes:
        click.confirm("Continue?", abort=True)

    codepipeline.update_pipeline(
        pipeline=pipeline_branch.with_branch(pipeline_definition, branch)
    )
    click.echo("Pipeline updated.")

    if start_execution:
        execution = codepipeline.start_pipeline_execution(name=pipeline_name)
        click.echo(f"Started execution {execution['pipelineExecutionId']}")

    click.echo(pipeline_branch.console_link(pipeline_name, ctx.session.region_name))


@pipelines.command(
    help="Show the branch, latest execution and stage statuses of this repository's CodePipeline"
)
@with_pipeline_selection
@click.pass_obj
def status(ctx: AppContext, repository: str | None, pipeline_name: str | None) -> None:
    codepipeline = ctx.session.client("codepipeline")
    pipeline_name = pipeline_name or resolve_pipeline_name(codepipeline, repository)
    pipeline_definition = codepipeline.get_pipeline(name=pipeline_name)["pipeline"]

    click.echo(f"Pipeline: {pipeline_name}")
    click.echo(f"Branch:   {pipeline_branch.configured_branch(pipeline_definition)}")
    click.echo()

    executions = codepipeline.list_pipeline_executions(
        pipelineName=pipeline_name, maxResults=1
    )["pipelineExecutionSummaries"]
    if executions:
        latest_execution = executions[0]
        trigger_type = latest_execution.get("trigger", {}).get(
            "triggerType", "unknown trigger"
        )
        started = pipeline_branch.format_timestamp(latest_execution.get("startTime"))
        click.echo(
            f"Latest execution: {latest_execution['status']} "
            f"(started {started}, {trigger_type})"
        )
        source_revisions = latest_execution.get("sourceRevisions", [])
        if source_revisions:
            click.echo(
                f"Commit:           {pipeline_branch.commit_summary(source_revisions[0])}"
            )
    else:
        click.echo("Latest execution: none")
    click.echo()

    stage_table = Table(show_header=True, header_style="bold cyan")
    stage_table.add_column("Stage")
    stage_table.add_column("Status")
    stage_table.add_column("Last change", no_wrap=True)
    stage_states = codepipeline.get_pipeline_state(name=pipeline_name)["stageStates"]
    for stage_state in stage_states:
        stage_table.add_row(
            stage_state["stageName"],
            stage_state.get("latestExecution", {}).get("status", "-"),
            pipeline_branch.format_timestamp(
                pipeline_branch.stage_last_change(stage_state)
            ),
        )
    Console().print(stage_table)
    click.echo(pipeline_branch.console_link(pipeline_name, ctx.session.region_name))


def resolve_pipeline_name(
    codepipeline: CodePipelineClient, repository: str | None
) -> str:
    repository = repository or pipeline_branch.git_remote_repository()
    if not repository:
        raise click.UsageError(
            "Could not determine the repository from the git remote origin. "
            "Use --repository or --pipeline."
        )
    click.echo(f"Looking up pipeline for {repository}...", err=True)
    matching_pipelines = pipeline_branch.find_pipelines_for_repository(
        codepipeline, repository
    )
    if not matching_pipelines:
        raise click.ClickException(
            f"No pipeline found with source repository {repository}."
        )
    if len(matching_pipelines) > 1:
        raise click.ClickException(
            f"Several pipelines use {repository}, pick one with --pipeline: "
            + ", ".join(matching_pipelines)
        )
    return matching_pipelines[0]
