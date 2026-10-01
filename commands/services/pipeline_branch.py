import copy
import json
import os
import re
import subprocess
from datetime import UTC, datetime

from mypy_boto3_codepipeline.client import CodePipelineClient
from mypy_boto3_codepipeline.type_defs import (
    ActionDeclarationOutputTypeDef,
    PipelineDeclarationOutputTypeDef,
    SourceRevisionTypeDef,
    StageStateTypeDef,
)

GITHUB_SOURCE_PROVIDER = "CodeStarSourceConnection"
DEFAULT_REGION = "eu-west-1"
LS_REMOTE_NO_MATCHING_REFS = 2


def github_source_actions(
    pipeline_definition: PipelineDeclarationOutputTypeDef,
) -> list[ActionDeclarationOutputTypeDef]:
    return [
        action
        for stage in pipeline_definition["stages"]
        for action in stage["actions"]
        if action["actionTypeId"]["provider"] == GITHUB_SOURCE_PROVIDER
    ]


def configured_branch(
    pipeline_definition: PipelineDeclarationOutputTypeDef,
) -> str | None:
    source_actions = github_source_actions(pipeline_definition)
    if not source_actions:
        return None
    return source_actions[0]["configuration"].get("BranchName")


def source_repository(
    pipeline_definition: PipelineDeclarationOutputTypeDef,
) -> str | None:
    source_actions = github_source_actions(pipeline_definition)
    if not source_actions:
        return None
    return source_actions[0]["configuration"].get("FullRepositoryId")


def uses_repository(
    pipeline_definition: PipelineDeclarationOutputTypeDef, repository: str
) -> bool:
    return any(
        action["configuration"].get("FullRepositoryId", "").lower()
        == repository.lower()
        for action in github_source_actions(pipeline_definition)
    )


def find_pipelines_for_repository(
    codepipeline: CodePipelineClient, repository: str
) -> list[str]:
    pipeline_names = [
        pipeline["name"]
        for page in codepipeline.get_paginator("list_pipelines").paginate()
        for pipeline in page["pipelines"]
    ]
    return [
        pipeline_name
        for pipeline_name in pipeline_names
        if uses_repository(
            codepipeline.get_pipeline(name=pipeline_name)["pipeline"], repository
        )
    ]


def with_branch(
    pipeline_definition: PipelineDeclarationOutputTypeDef, branch: str
) -> PipelineDeclarationOutputTypeDef:
    """
    Sets BranchName on the GitHub source actions and drops the Git triggers, so
    CodePipeline regenerates the default push trigger for the new branch.
    """
    updated_definition = copy.deepcopy(pipeline_definition)
    for action in github_source_actions(updated_definition):
        action["configuration"]["BranchName"] = branch
    updated_definition.pop("triggers", None)
    return updated_definition


def commit_summary(source_revision: SourceRevisionTypeDef) -> str:
    revision_id = source_revision.get("revisionId", "")[:10]
    revision_summary = source_revision.get("revisionSummary", "")
    try:
        message = json.loads(revision_summary)["CommitMessage"]
    except ValueError, KeyError, TypeError:
        message = revision_summary
    first_line = message.splitlines()[0] if message else ""
    return f"{revision_id} {first_line}"


def format_timestamp(timestamp: datetime | None) -> str:
    if timestamp is None:
        return "-"
    return timestamp.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def stage_last_change(stage_state: StageStateTypeDef) -> datetime | None:
    action_changes = [
        action_state["latestExecution"]["lastStatusChange"]
        for action_state in stage_state.get("actionStates", [])
        if "lastStatusChange" in action_state.get("latestExecution", {})
    ]
    return max(action_changes, default=None)


def console_link(pipeline_name: str, region: str | None) -> str:
    region = region or DEFAULT_REGION
    return (
        f"https://{region}.console.aws.amazon.com/codesuite/codepipeline/pipelines/"
        f"{pipeline_name}/view?region={region}"
    )


def repository_from_remote_url(remote_url: str) -> str | None:
    match = re.search(r"[:/]([^/:]+)/([^/]+?)(?:\.git)?/?$", remote_url.strip())
    return f"{match.group(1)}/{match.group(2)}" if match else None


class BranchCheckError(Exception):
    pass


def _run_git(*arguments: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *arguments],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )


def git_remote_repository() -> str | None:
    result = _run_git("remote", "get-url", "origin")
    if result.returncode != 0:
        return None
    return repository_from_remote_url(result.stdout)


def current_git_branch() -> str | None:
    result = _run_git("branch", "--show-current")
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def branch_exists_in_repository(repository: str, branch: str) -> bool:
    """
    Checks the GitHub repository itself rather than the origin of the current
    directory, so the check holds for --repository and --pipeline too.
    """
    result = _run_git(
        "ls-remote",
        "--exit-code",
        "--heads",
        f"https://github.com/{repository}.git",
        branch,
    )
    if result.returncode == LS_REMOTE_NO_MATCHING_REFS:
        return False
    if result.returncode != 0:
        raise BranchCheckError(
            f"Could not list branches of {repository}: {result.stderr.strip()}"
        )
    return True
