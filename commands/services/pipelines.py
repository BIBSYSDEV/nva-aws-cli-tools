import boto3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from rich.text import Text
from typing import Optional
import json


@dataclass
class ExecutionDetails:
    execution_id: str
    status: str = "Unknown"
    last_change: Optional[datetime] = None
    commit_id: Optional[str] = None
    commit_message: Optional[str] = None

    def get_status_text(self) -> Text:
        if self.status == "Succeeded":
            return Text("✔ Succeeded", style="green")
        elif self.status == "InProgress":
            return Text("In progress...", style="yellow")
        elif self.status == "Failed":
            return Text("✘ Failed", style="red")
        else:
            return Text("✘ Unknown", style="red")

    def get_last_change(self) -> str:
        if self.last_change is None:
            return "Unknown"
        else:
            return self.last_change.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")


@dataclass
class PipelineDetails:
    pipeline_name: str
    last_run: ExecutionDetails
    last_deploy: ExecutionDetails
    repository: Optional[str] = field(default="Unknown")
    branch: Optional[str] = field(default="Unknown")
    summary: Optional[str] = field(default="Unknown")

    def __post_init__(self):
        if self.repository is None:
            self.repository = "Unknown"
        if self.branch is None:
            self.branch = "Unknown"
        if self.last_run is None:
            self.last_run = ExecutionDetails(execution_id="Unknown")
        if self.last_deploy is None:
            self.last_deploy = ExecutionDetails(execution_id="Unknown")

    def get_link_to_last_commit(self) -> str:
        timestamp = self.last_run.get_last_change()
        message = self.last_run.commit_message
        commit_id = self.last_run.commit_id
        base_url = f"https://github.com/{self.repository}/commit/{commit_id}"
        return f"{timestamp}: [link={base_url}]{message}[/link]"

    def get_link_to_deployed_commit(self) -> str:
        timestamp = self.last_deploy.get_last_change()
        message = self.last_deploy.commit_message
        commit_id = self.last_deploy.commit_id
        base_url = f"https://github.com/{self.repository}/commit/{commit_id}"
        return f"{timestamp}: [link={base_url}]{message}[/link]"

    def get_status_text(self) -> Text:
        if self.last_run is None or self.last_deploy is None:
            return Text("✘ Unknown", style="red")
        stage_statuses = [self.last_run.status, self.last_deploy.status]
        if self.is_in_sync() and all(
            status == "Succeeded" for status in stage_statuses
        ):
            return Text("✔ OK", style="green")
        elif any(status == "InProgress" for status in stage_statuses):
            return Text("In progress...", style="yellow")
        else:
            status_message = "/".join(stage_statuses)
            return Text("✘ " + status_message, style="red")

    def is_in_sync(self) -> bool:
        """
        Check if the pipeline stages are in sync.
        """
        return self.last_run.execution_id == self.last_deploy.execution_id


def get_summary(source_action: dict) -> str:
    """
    Extracts the summary information from the source stage.
    """
    summary = source_action.get("latestExecution", {}).get("summary", "")
    if summary.startswith('{"ProviderType":"GitHub"'):
        commit_message = json.loads(summary)["CommitMessage"]
        cleaned_message = commit_message.replace("\n", " ").strip()
        return cleaned_message
    return summary


def get_git_details(summary: dict) -> tuple[str, str]:
    """
    Extracts Git commit details (commit hash and message).
    """
    revisions = summary.get("sourceRevisions", [])
    if len(revisions) > 0:
        action = revisions[0]
        revision_id = action.get("revisionId", "")
        summary = action.get("revisionSummary", "")
        if summary.startswith('{"ProviderType":"GitHub"'):
            commit_message = json.loads(summary)["CommitMessage"]
            summary = commit_message.replace("\n", " ").strip()
        return revision_id, summary
    return "Unknown", "Unknown"


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


def get_details_from_pipeline_execution(pipeline_runs: dict) -> Optional[ExecutionDetails]:
    """
    Extracts the execution details from a pipeline summary.
    """
    executions = pipeline_runs.get("pipelineExecutionSummaries", [])
    if not len(executions) > 0:
        return None

    pipeline_summary = executions[0]
    execution_id = pipeline_summary.get("pipelineExecutionId")
    status = pipeline_summary.get("status", "Unknown")
    last_change = pipeline_summary.get("lastUpdateTime")
    commit_id, commit_message = get_git_details(pipeline_summary)
    return ExecutionDetails(
        execution_id=execution_id,
        status=status,
        last_change=last_change,
        commit_id=commit_id,
        commit_message=commit_message,
    )


def get_single_pipeline_details(
    pipeline_name: str, codepipeline_client
) -> Optional[PipelineDetails]:
    # Get current source details
    pipeline_state = codepipeline_client.get_pipeline_state(name=pipeline_name)
    stages = {stage["stageName"]: stage for stage in pipeline_state["stageStates"]}
    branch_name, repo_name = get_source_details(stages.get("Source", {}))

    if branch_name is None:
        # Skipping irrelevant pipelines with no source details
       return None

    # Check last pipeline run
    last_run = codepipeline_client.list_pipeline_executions(
        pipelineName=pipeline_name, maxResults=1
    )
    last_run_details = get_details_from_pipeline_execution(last_run)

    # Check last successful deploy
    last_deploy = codepipeline_client.list_pipeline_executions(
        pipelineName=pipeline_name,
        maxResults=1,
        filter={"succeededInStage": {"stageName": "Deploy"}},
    )
    last_deploy_details = get_details_from_pipeline_execution(last_deploy)

    return PipelineDetails(
        pipeline_name=pipeline_name,
        repository=repo_name,
        branch=branch_name,
        last_run=last_run_details,
        last_deploy=last_deploy_details,
    )


def get_pipeline_details_for_account(profile: str) -> list[PipelineDetails]:
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    codepipeline = session.client("codepipeline")

    pipelines = codepipeline.list_pipelines()["pipelines"]
    results: list[PipelineDetails] = []

    for pipeline in pipelines:
        pipeline_name = pipeline["name"]
        try:
            pipeline_details = get_single_pipeline_details(pipeline_name, codepipeline)
            if pipeline_details:
                results.append(pipeline_details)
        except Exception as e:
            print(f"Error fetching details for pipeline {pipeline_name}: {e}")
            continue
    return results
