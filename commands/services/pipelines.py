import boto3
from dataclasses import dataclass, field
from datetime import datetime
from rich.text import Text
from rich.pretty import pprint


@dataclass
class StageDetails:
    execution_id: str
    last_change: datetime
    status: str = field(default="Unknown")


@dataclass
class SourceDetails:
    execution_id: str
    repository: str
    branch: str
    status: str = field(default="Unknown")


@dataclass
class BuildDetails:
    execution_id: str
    built_at: datetime
    status: str = field(default="Unknown")


@dataclass
class DeployDetails:
    execution_id: str
    deployed_at: datetime
    status: str = field(default="Unknown")


@dataclass
class PipelineDetails:
    pipeline_name: str
    source: SourceDetails
    build: BuildDetails
    deploy: DeployDetails

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


def get_stage_status(stage_state: dict) -> tuple[str, str]:
    """
    Extracts the status and execution ID from the stage state.
    """
    latest_execution = stage_state.get("latestExecution")
    if latest_execution is None:
        print("Error: No latest execution found for stage.")
    execution_id = latest_execution.get("pipelineExecutionId")
    if execution_id is None:
        print("Error: No execution ID found for stage.")
    status = latest_execution.get("status", "Unknown")
    return execution_id, status


def get_source_details(stage_state: dict) -> SourceDetails:
    """
    Extracts the source information from the stage state.
    """
    execution_id, status = get_stage_status(stage_state)

    action_states = stage_state.get("actionStates")
    if action_states is None:
        print("Error: No action states found for source stage.")
        return None
    entity_url = action_states[0].get("entityUrl", "")
    if "Branch=" in entity_url:
        git_branch = entity_url.split("Branch=")[-1].split("&")[0]
    if "FullRepositoryId=" in entity_url:
        repo_name = entity_url.split("FullRepositoryId=")[-1].split("&")[0]
    return SourceDetails(
        execution_id=execution_id,
        status=status,
        repository=repo_name,
        branch=git_branch,
    )


def get_build_details(stage_state: dict) -> BuildDetails:
    """
    Extracts the build information from the stage state.
    """
    execution_id, status = get_stage_status(stage_state)
    action_states = stage_state.get("actionStates")
    if action_states is None is None:
        print("Error: No action states found for build stage.")
        return None
    action_details = action_states[0].get("latestExecution", {})

    return BuildDetails(
        execution_id=execution_id,
        status=status,
        built_at=action_details.get("lastStatusChange"),
    )


def get_deploy_details(stage_state: dict) -> DeployDetails:
    """
    Extracts the deployment information from the stage state.
    """
    execution_id, status = get_stage_status(stage_state)
    action_states = stage_state.get("actionStates")
    if action_states is None is None:
        print("Error: No action states or latest execution found for deployment stage.")
        return None
    action_details = action_states[0].get("latestExecution", {})

    return DeployDetails(
        execution_id=execution_id,
        status=status,
        deployed_at=action_details.get("lastStatusChange"),
    )


def get_single_pipeline_details(pipeline_name: str, stages: dict) -> PipelineDetails:

    source_details = get_source_details(stages["Source"])
    build_details = get_build_details(stages["Build"])
    deploy_details = get_deploy_details(stages["Deploy"])
    return PipelineDetails(
        pipeline_name=pipeline_name,
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
