from unittest.mock import MagicMock, patch

import boto3
from click.testing import CliRunner
from moto import mock_aws

from cli import cli


def _build_session_with_stubbed_codepipeline(fake_codepipeline) -> boto3.Session:
    session = boto3.Session()
    real_client = session.client
    session.client = lambda name, *args, **kwargs: (
        fake_codepipeline
        if name == "codepipeline"
        else real_client(name, *args, **kwargs)
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
