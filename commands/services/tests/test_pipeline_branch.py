import json
import subprocess
from datetime import UTC, datetime, timedelta, timezone
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
from mypy_boto3_codepipeline.type_defs import (
    PipelineDeclarationOutputTypeDef,
    SourceRevisionTypeDef,
    StageStateTypeDef,
)

from commands.services import pipeline_branch


def _pipeline_definition(
    repository="BIBSYSDEV/nva-foo", branch="main"
) -> PipelineDeclarationOutputTypeDef:
    definition = {
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
                            "BranchName": branch,
                        },
                    }
                ],
            },
            {
                "name": "Deploy",
                "actions": [
                    {
                        "name": "Deploy",
                        "actionTypeId": {"provider": "CloudFormation"},
                        "configuration": {},
                    }
                ],
            },
        ],
        "triggers": [{"providerType": "CodeStarSourceConnection"}],
    }
    return cast(PipelineDeclarationOutputTypeDef, definition)


def test_configured_branch_reads_branch_from_github_source_action():
    assert pipeline_branch.configured_branch(_pipeline_definition()) == "main"


def test_configured_branch_is_none_without_github_source_action():
    definition = cast(PipelineDeclarationOutputTypeDef, {"stages": [{"actions": []}]})

    assert pipeline_branch.configured_branch(definition) is None


def test_source_repository_reads_repository_from_github_source_action():
    definition = _pipeline_definition(repository="BIBSYSDEV/nva-foo")

    assert pipeline_branch.source_repository(definition) == "BIBSYSDEV/nva-foo"


def test_source_repository_is_none_without_github_source_action():
    definition = cast(PipelineDeclarationOutputTypeDef, {"stages": [{"actions": []}]})

    assert pipeline_branch.source_repository(definition) is None


def test_uses_repository_ignores_case():
    definition = _pipeline_definition(repository="BIBSYSDEV/nva-foo")

    assert pipeline_branch.uses_repository(definition, "bibsysdev/NVA-FOO")
    assert not pipeline_branch.uses_repository(definition, "bibsysdev/nva-bar")


def test_with_branch_sets_branch_and_drops_triggers_without_mutating_input():
    original = _pipeline_definition(branch="main")

    updated = pipeline_branch.with_branch(original, "feature")

    assert pipeline_branch.configured_branch(updated) == "feature"
    assert "triggers" not in updated
    assert pipeline_branch.configured_branch(original) == "main"
    assert "triggers" in original


def test_find_pipelines_for_repository_returns_only_matching_pipelines():
    definitions = {
        "foo-pipeline": _pipeline_definition(repository="BIBSYSDEV/nva-foo"),
        "bar-pipeline": _pipeline_definition(repository="BIBSYSDEV/nva-bar"),
    }
    codepipeline = MagicMock()
    codepipeline.get_paginator.return_value.paginate.return_value = [
        {"pipelines": [{"name": "foo-pipeline"}]},
        {"pipelines": [{"name": "bar-pipeline"}]},
    ]
    codepipeline.get_pipeline.side_effect = lambda name: {"pipeline": definitions[name]}

    matches = pipeline_branch.find_pipelines_for_repository(
        codepipeline, "BIBSYSDEV/nva-foo"
    )

    assert matches == ["foo-pipeline"]


def test_commit_summary_uses_first_line_of_github_commit_message():
    source_revision: SourceRevisionTypeDef = {
        "actionName": "Source",
        "revisionId": "0123456789abcdef",
        "revisionSummary": json.dumps(
            {"ProviderType": "GitHub", "CommitMessage": "Fix bug\n\nLong body"}
        ),
    }

    assert pipeline_branch.commit_summary(source_revision) == "0123456789 Fix bug"


def test_commit_summary_falls_back_to_plain_revision_summary():
    source_revision: SourceRevisionTypeDef = {
        "actionName": "Source",
        "revisionId": "abc",
        "revisionSummary": "Plain summary",
    }

    assert pipeline_branch.commit_summary(source_revision) == "abc Plain summary"


def test_console_link_defaults_region():
    assert pipeline_branch.console_link("foo-pipeline", None) == (
        "https://eu-west-1.console.aws.amazon.com/codesuite/codepipeline/pipelines/"
        "foo-pipeline/view?region=eu-west-1"
    )


@pytest.mark.parametrize(
    "remote_url",
    [
        "git@github.com:BIBSYSDEV/nva-foo.git",
        "https://github.com/BIBSYSDEV/nva-foo.git\n",
        "https://github.com/BIBSYSDEV/nva-foo",
        "ssh://git@github.com/BIBSYSDEV/nva-foo.git",
    ],
)
def test_repository_from_remote_url_handles_ssh_and_https(remote_url):
    assert pipeline_branch.repository_from_remote_url(remote_url) == "BIBSYSDEV/nva-foo"


def test_repository_from_remote_url_returns_none_for_unparseable_url():
    assert pipeline_branch.repository_from_remote_url("not-a-url") is None


def _completed(returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=stderr
    )


def _fake_git(result: subprocess.CompletedProcess):
    return patch.object(pipeline_branch.subprocess, "run", return_value=result)


def test_git_remote_repository_parses_origin_url():
    with _fake_git(_completed(stdout="git@github.com:BIBSYSDEV/nva-foo.git\n")):
        assert pipeline_branch.git_remote_repository() == "BIBSYSDEV/nva-foo"


def test_current_git_branch_strips_output():
    with _fake_git(_completed(stdout="feature\n")):
        assert pipeline_branch.current_git_branch() == "feature"


def test_git_helpers_return_none_outside_git_repository():
    with _fake_git(_completed(returncode=128)):
        assert pipeline_branch.git_remote_repository() is None
        assert pipeline_branch.current_git_branch() is None


def test_branch_exists_in_repository_queries_github_repository():
    with _fake_git(_completed(stdout="abc\trefs/heads/feature\n")) as fake_run:
        assert pipeline_branch.branch_exists_in_repository(
            "BIBSYSDEV/nva-foo", "feature"
        )

    git_arguments = fake_run.call_args.args[0]
    assert "https://github.com/BIBSYSDEV/nva-foo.git" in git_arguments
    assert "origin" not in git_arguments
    assert fake_run.call_args.kwargs["env"]["GIT_TERMINAL_PROMPT"] == "0"


def test_branch_exists_in_repository_is_false_for_missing_branch():
    with _fake_git(_completed(returncode=2)):
        assert not pipeline_branch.branch_exists_in_repository(
            "BIBSYSDEV/nva-foo", "missing"
        )


def test_branch_exists_in_repository_raises_when_repository_is_unreachable():
    unreachable = _completed(returncode=128, stderr="fatal: repository not found\n")

    with (
        _fake_git(unreachable),
        pytest.raises(pipeline_branch.BranchCheckError) as error,
    ):
        pipeline_branch.branch_exists_in_repository("BIBSYSDEV/nva-foo", "main")

    assert "BIBSYSDEV/nva-foo: fatal: repository not found" in str(error.value)


def test_current_git_branch_is_none_on_detached_head():
    with patch.object(pipeline_branch.subprocess, "run", return_value=_completed()):
        assert pipeline_branch.current_git_branch() is None


def test_format_timestamp_converts_to_utc():
    oslo_summer_time = timezone(timedelta(hours=2))

    formatted = pipeline_branch.format_timestamp(
        datetime(2026, 9, 25, 12, 30, tzinfo=oslo_summer_time)
    )

    assert formatted == "2026-09-25 10:30 UTC"


def test_format_timestamp_shows_dash_when_missing():
    assert pipeline_branch.format_timestamp(None) == "-"


def test_stage_last_change_is_latest_action_change():
    stage_state = cast(
        StageStateTypeDef,
        {
            "stageName": "Deploy",
            "actionStates": [
                {
                    "latestExecution": {
                        "lastStatusChange": datetime(2026, 9, 25, 9, 0, tzinfo=UTC)
                    }
                },
                {
                    "latestExecution": {
                        "lastStatusChange": datetime(2026, 9, 25, 11, 0, tzinfo=UTC)
                    }
                },
                {"latestExecution": {}},
                {},
            ],
        },
    )

    assert pipeline_branch.stage_last_change(stage_state) == datetime(
        2026, 9, 25, 11, 0, tzinfo=UTC
    )


def test_stage_last_change_is_none_for_stage_that_never_ran():
    stage_state = cast(StageStateTypeDef, {"stageName": "Deploy"})

    assert pipeline_branch.stage_last_change(stage_state) is None
