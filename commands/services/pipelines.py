import boto3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from rich.text import Text
from typing import Optional


@dataclass
class ExecutionDetails:
    execution_id: str
    last_change: datetime
    status: str = "Unknown"

    def get_last_change(self) -> str:
        if self.last_change is None:
            return "Unknown"
        else:
            return self.last_change.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")

    def get_status_text(self) -> Text:
        if self.status == "Succeeded":
            return Text("✔ Succeeded", style="green")
        elif self.status == "InProgress":
            return Text("In progress...", style="yellow")
        elif self.status == "Failed":
            return Text("✘ Failed", style="red")
        else:
            return Text("✘ Unknown", style="red")


@dataclass
class PipelineDetails:
    pipeline_name: str
    source: ExecutionDetails
    build: ExecutionDetails
    deploy: ExecutionDetails
    repository: Optional[str] = field(default="Unknown")
    branch: Optional[str] = field(default="Unknown")

    def __post_init__(self):
        if self.repository is None:
            self.repository = "Unknown"
        if self.branch is None:
            self.branch = "Unknown"

    def get_status_text(self) -> Text:
        stage_statuses = [self.source.status, self.build.status, self.deploy.status]
        if self.is_in_sync() and all(
            status == "Succeeded" for status in stage_statuses
        ):
            return Text("✔", style="green")
        else:
            return Text("✘", style="red")

    def is_in_sync(self) -> bool:
        """
        Check if the pipeline stages are in sync.
        """
        return (
            self.source.execution_id
            == self.build.execution_id
            == self.deploy.execution_id
        )


def get_execution_details(stage_state: dict) -> ExecutionDetails:
    """
    Extracts the execution details for a single stage.
    """
    latest_execution = stage_state.get("latestExecution", {})
    execution_id = latest_execution.get("pipelineExecutionId")
    status = latest_execution.get("status", "Unknown")

    action_states = stage_state.get("actionStates", [])
    action_timestamp = None
    if len(action_states) > 0:
        action_details = action_states[0].get("latestExecution", {})
        action_timestamp = action_details.get("lastStatusChange")

    return ExecutionDetails(
        status=status, execution_id=execution_id, last_change=action_timestamp
    )


def get_source_details(stage_state: dict) -> tuple[str, str]:
    """
    Extracts the source information (branch/repo) from the source stage.
    """
    branch_name = None
    repo_name = None
    action_states = stage_state.get("actionStates", [])
    if len(action_states) > 0:
        entity_url = action_states[0].get("entityUrl", "")
        if "Branch=" in entity_url:
            branch_name = entity_url.split("Branch=")[-1].split("&")[0]
        if "FullRepositoryId=" in entity_url:
            repo_name = entity_url.split("FullRepositoryId=")[-1].split("&")[0]
    return branch_name, repo_name


def get_single_pipeline_details(pipeline_name: str, stages: dict) -> PipelineDetails:
    branch_name, repo_name = get_source_details(stages.get("Source", {}))
    source_details = get_execution_details(stages.get("Source", {}))
    build_details = get_execution_details(stages.get("Build", {}))
    deploy_details = get_execution_details(stages.get("Deploy", {}))
    return PipelineDetails(
        pipeline_name=pipeline_name,
        repository=repo_name,
        branch=branch_name,
        source=source_details,
        build=build_details,
        deploy=deploy_details,
    )


def get_pipeline_details_for_account(profile: str) -> list[PipelineDetails]:
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    codepipeline = session.client("codepipeline")

    pipelines = codepipeline.list_pipelines()["pipelines"]
    results: list[PipelineDetails] = []

    for pipeline in pipelines:
        pipeline_name = pipeline["name"]
        response = codepipeline.get_pipeline_state(name=pipeline_name)
        stages = {stage["stageName"]: stage for stage in response["stageStates"]}
        pipeline_details = get_single_pipeline_details(pipeline_name, stages)
        results.append(pipeline_details)
    return results
