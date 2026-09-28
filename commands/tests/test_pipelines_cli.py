from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import MagicMock, patch

import boto3
from click.testing import CliRunner
from moto import mock_aws

from cli import cli

OSLO_SUMMER_TIME = timezone(timedelta(hours=2))


def _build_session_with_stubbed_codepipeline(fake_codepipeline) -> boto3.Session:
    session = boto3.Session()
    real_client = session.client
    session.client = cast(
        Any,
        lambda name, *args, **kwargs: (
            fake_codepipeline
            if name == "codepipeline"
            else real_client(name, *args, **kwargs)
        ),
    )
    return session


@mock_aws
def test_pipelines_branches_renders_pipeline_with_source_details():
    boto3.client("iam").create_account_alias(AccountAlias="nva-test")
    fake_codepipeline = MagicMock()
    fake_codepipeline.list_pipelines.return_value = {
        "pipelines": [{"name": "test-pipeline"}]
    }
    fake_codepipeline.get_pipeline_state.return_value = {
        "stageStates": [
            {
                "stageName": "Source",
                "actionStates": [
                    {
                        "entityUrl": "https://example.org/?Branch=develop&FullRepositoryId=org/repo"
                    }
                ],
            }
        ]
    }
    fake_codepipeline.list_pipeline_executions.return_value = {
        "pipelineExecutionSummaries": []
    }

    with patch(
        "cli.build_session",
        return_value=_build_session_with_stubbed_codepipeline(fake_codepipeline),
    ):
        result = CliRunner().invoke(cli, ["--quiet", "pipelines", "branches"])

    assert result.exit_code == 0, result.exception
    assert "org/repo" in result.output
    assert "develop" in result.output
    assert "nva-test" in result.output


@mock_aws
def test_pipelines_branches_skips_pipelines_without_source_details():
    boto3.client("iam").create_account_alias(AccountAlias="nva-test")
    fake_codepipeline = MagicMock()
    fake_codepipeline.list_pipelines.return_value = {
        "pipelines": [{"name": "irrelevant"}]
    }
    fake_codepipeline.get_pipeline_state.return_value = {
        "stageStates": [{"stageName": "Source", "actionStates": [{"entityUrl": ""}]}]
    }
    fake_codepipeline.list_pipeline_executions.return_value = {
        "pipelineExecutionSummaries": []
    }

    with patch(
        "cli.build_session",
        return_value=_build_session_with_stubbed_codepipeline(fake_codepipeline),
    ):
        result = CliRunner().invoke(cli, ["--quiet", "pipelines", "branches"])

    assert result.exit_code == 0, result.exception
    assert "irrelevant" not in result.output
    assert "0 pipelines" in result.output


def _fake_codepipeline_for_repository(repository="BIBSYSDEV/nva-foo") -> MagicMock:
    fake_codepipeline = MagicMock()
    fake_codepipeline.get_paginator.return_value.paginate.return_value = [
        {"pipelines": [{"name": "foo-pipeline"}]}
    ]
    fake_codepipeline.get_pipeline.return_value = {
        "pipeline": {
            "name": "foo-pipeline",
            "stages": [
                {
                    "name": "Source",
                    "actions": [
                        {
                            "name": "Source",
                            "actionTypeId": {"provider": "CodeStarSourceConnection"},
                            "configuration": {
                                "FullRepositoryId": repository,
                                "BranchName": "main",
                            },
                        }
                    ],
                }
            ],
            "triggers": [{"providerType": "CodeStarSourceConnection"}],
        }
    }
    fake_codepipeline.start_pipeline_execution.return_value = {
        "pipelineExecutionId": "execution-1"
    }
    return fake_codepipeline


def _invoke_with_git(
    fake_codepipeline,
    arguments,
    remote_repository: str | None = "BIBSYSDEV/nva-foo",
    current_branch: str | None = "feature",
    branch_on_origin=True,
    **kwargs,
):
    with (
        patch(
            "cli.build_session",
            return_value=_build_session_with_stubbed_codepipeline(fake_codepipeline),
        ),
        patch(
            "commands.services.pipeline_branch.git_remote_repository",
            return_value=remote_repository,
        ),
        patch(
            "commands.services.pipeline_branch.current_git_branch",
            return_value=current_branch,
        ),
        patch("commands.services.pipeline_branch.is_git_repository", return_value=True),
        patch(
            "commands.services.pipeline_branch.branch_exists_on_origin",
            return_value=branch_on_origin,
        ),
    ):
        return CliRunner().invoke(cli, ["--quiet", "pipelines", *arguments], **kwargs)


@mock_aws
def test_pipelines_deploy_points_pipeline_at_current_branch_and_starts_execution():
    boto3.client("iam").create_account_alias(AccountAlias="nva-test")
    fake_codepipeline = _fake_codepipeline_for_repository()

    result = _invoke_with_git(fake_codepipeline, ["deploy", "--yes"])

    assert result.exit_code == 0, result.output
    updated_pipeline = fake_codepipeline.update_pipeline.call_args.kwargs["pipeline"]
    source_configuration = updated_pipeline["stages"][0]["actions"][0]["configuration"]
    assert source_configuration["BranchName"] == "feature"
    assert "triggers" not in updated_pipeline
    fake_codepipeline.start_pipeline_execution.assert_called_once_with(
        name="foo-pipeline"
    )
    assert "nva-test" in result.output
    assert "main -> feature" in result.output
    assert "Started execution execution-1" in result.output


@mock_aws
def test_pipelines_deploy_with_no_start_only_updates_pipeline():
    boto3.client("iam").create_account_alias(AccountAlias="nva-test")
    fake_codepipeline = _fake_codepipeline_for_repository()

    result = _invoke_with_git(
        fake_codepipeline,
        [
            "deploy",
            "--yes",
            "--no-start",
            "--branch",
            "main",
            "--pipeline",
            "foo-pipeline",
        ],
    )

    assert result.exit_code == 0, result.output
    fake_codepipeline.update_pipeline.assert_called_once()
    fake_codepipeline.start_pipeline_execution.assert_not_called()
    fake_codepipeline.get_paginator.assert_not_called()


@mock_aws
def test_pipelines_deploy_aborts_when_confirmation_is_declined():
    boto3.client("iam").create_account_alias(AccountAlias="nva-test")
    fake_codepipeline = _fake_codepipeline_for_repository()

    result = _invoke_with_git(fake_codepipeline, ["deploy"], input="n\n")

    assert result.exit_code == 1
    fake_codepipeline.update_pipeline.assert_not_called()


def test_pipelines_deploy_refuses_branch_missing_on_origin():
    fake_codepipeline = _fake_codepipeline_for_repository()

    result = _invoke_with_git(
        fake_codepipeline, ["deploy", "--yes"], branch_on_origin=False
    )

    assert result.exit_code == 1
    assert "does not exist on origin" in result.output
    fake_codepipeline.update_pipeline.assert_not_called()


def test_pipelines_deploy_fails_when_no_pipeline_uses_repository():
    fake_codepipeline = _fake_codepipeline_for_repository(repository="BIBSYSDEV/other")

    result = _invoke_with_git(fake_codepipeline, ["deploy", "--yes"])

    assert result.exit_code == 1
    assert "No pipeline found with source repository BIBSYSDEV/nva-foo" in result.output


def test_pipelines_deploy_fails_when_several_pipelines_use_repository():
    fake_codepipeline = _fake_codepipeline_for_repository()
    fake_codepipeline.get_paginator.return_value.paginate.return_value = [
        {"pipelines": [{"name": "foo-pipeline"}, {"name": "foo-pipeline-copy"}]}
    ]

    result = _invoke_with_git(fake_codepipeline, ["deploy", "--yes"])

    assert result.exit_code == 1
    assert "foo-pipeline, foo-pipeline-copy" in result.output


def test_pipelines_deploy_requires_repository_outside_git_repository():
    fake_codepipeline = _fake_codepipeline_for_repository()

    result = _invoke_with_git(
        fake_codepipeline, ["deploy", "--yes"], remote_repository=None
    )

    assert result.exit_code == 2
    assert "Use --repository or --pipeline" in result.output


def test_pipelines_deploy_requires_branch_on_detached_head():
    fake_codepipeline = _fake_codepipeline_for_repository()

    result = _invoke_with_git(
        fake_codepipeline, ["deploy", "--yes"], current_branch=None
    )

    assert result.exit_code == 2
    assert "Use --branch" in result.output


def test_pipelines_status_shows_branch_latest_execution_and_stages():
    fake_codepipeline = _fake_codepipeline_for_repository()
    fake_codepipeline.list_pipeline_executions.return_value = {
        "pipelineExecutionSummaries": [
            {
                "status": "Succeeded",
                "startTime": datetime(2026, 9, 25, 12, 0, tzinfo=OSLO_SUMMER_TIME),
                "trigger": {"triggerType": "Webhook"},
                "sourceRevisions": [
                    {
                        "revisionId": "0123456789abcdef",
                        "revisionSummary": '{"ProviderType":"GitHub","CommitMessage":"Fix [bug]"}',
                    }
                ],
            }
        ]
    }
    fake_codepipeline.get_pipeline_state.return_value = {
        "stageStates": [
            {
                "stageName": "Source",
                "latestExecution": {"status": "Succeeded"},
                "actionStates": [
                    {
                        "latestExecution": {
                            "lastStatusChange": datetime(2026, 9, 25, 10, 1, tzinfo=UTC)
                        }
                    },
                    {
                        "latestExecution": {
                            "lastStatusChange": datetime(2026, 9, 25, 10, 2, tzinfo=UTC)
                        }
                    },
                ],
            },
            {"stageName": "Deploy"},
        ]
    }

    result = _invoke_with_git(fake_codepipeline, ["status"])

    assert result.exit_code == 0, result.output
    assert "Branch:   main" in result.output
    assert "Succeeded (started 2026-09-25 10:00 UTC, Webhook)" in result.output
    assert "0123456789 Fix [bug]" in result.output
    assert "2026-09-25 10:02 UTC" in result.output
    assert "2026-09-25 10:01 UTC" not in result.output
    assert "Deploy" in result.output
    assert "view?region=" in result.output


def test_pipelines_status_handles_pipeline_without_executions():
    fake_codepipeline = _fake_codepipeline_for_repository()
    fake_codepipeline.list_pipeline_executions.return_value = {
        "pipelineExecutionSummaries": []
    }
    fake_codepipeline.get_pipeline_state.return_value = {"stageStates": []}

    result = _invoke_with_git(
        fake_codepipeline, ["status", "--pipeline", "foo-pipeline"]
    )

    assert result.exit_code == 0, result.output
    assert "Latest execution: none" in result.output
