import boto3
from dataclasses import dataclass, field
from datetime import datetime
from rich.text import Text


@dataclass
class ExecutionDetails:
    execution_id: str
    last_change: datetime
    status: str = field(default="Unknown")


@dataclass
class PipelineDetails:
    pipeline_name: str
    repository: str
    branch: str
    source: ExecutionDetails
    build: ExecutionDetails
    deploy: ExecutionDetails

    def get_status_text(self) -> Text:
        if self.is_in_sync():
            if self.is_successful():
                return Text("✔ Succeeded", style="green")
            else:
                return Text("Unknown", style="red")
        else:
            return Text("In progress...", style="orange")

    def is_in_sync(self) -> bool:
        """
        Check if the pipeline stages are in sync.
        """
        return (
            self.source.execution_id
            == self.build.execution_id
            == self.deploy.execution_id
        )

    def is_successful(self) -> bool:
        """
        Check if the pipeline stages are successful.
        """
        statuses = [self.source.status, self.build.status, self.deploy.status]
        if any(status != "Succeeded" for status in statuses):
            return False
        return True


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
    branch_name, repo_name = get_source_details(stages["Source"])
    source_details = get_execution_details(stages["Source"])
    build_details = get_execution_details(stages["Build"])
    deploy_details = get_execution_details(stages["Deploy"])
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
