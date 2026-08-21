import io
import json
from typing import Any, cast
from unittest.mock import MagicMock, patch

import boto3
from click.testing import CliRunner
from moto import mock_aws

from cli import cli

FUNCTION_NAME = "master-pipelines-NvaPubli-ManuallyUpdatePublicatio-NFn4HKTz93o7"


def _arn(function_name: str) -> str:
    return f"arn:aws:lambda:eu-west-1:123456789012:function:{function_name}"


def _report(changes: list | None = None, **overrides) -> dict:
    report = {
        "dryRun": True,
        "type": "PUBLISHER",
        "oldValue": "OLD",
        "newValue": "NEW",
        "totalHits": 1,
        "hitsReturned": 1,
        "resourcesFetched": 1,
        "resourcesMatched": len(changes or []),
        "resourcesChanged": len(changes or []),
        "pagesFetched": 1,
        "limit": 10000,
        "limitReached": False,
        "pageSize": 100,
        "changes": changes or [],
    }
    return {**report, **overrides}


def _change(identifier: str = "id-1") -> dict:
    return {
        "identifier": identifier,
        "fieldChanges": [{"path": "publisher", "oldValue": "OLD", "newValue": "NEW"}],
    }


def _invoke_response(report: dict) -> dict:
    return {"Payload": io.BytesIO(json.dumps(report).encode("utf-8"))}


def _fake_lambda(invoke_responses: list[dict]) -> MagicMock:
    fake = MagicMock()
    fake.invoke.side_effect = invoke_responses
    return fake


def _fake_tagging(function_names: list[str]) -> MagicMock:
    fake = MagicMock()
    fake.get_paginator.return_value.paginate = lambda *args, **kwargs: iter(
        [
            {
                "ResourceTagMappingList": [
                    {"ResourceARN": _arn(name)} for name in function_names
                ]
            }
        ]
    )
    return fake


def _session(fake_lambda: MagicMock, fake_tagging: MagicMock) -> boto3.Session:
    session = boto3.Session()
    real_client = session.client

    def client(name, *args, **kwargs):
        if name == "lambda":
            return fake_lambda
        if name == "resourcegroupstaggingapi":
            return fake_tagging
        return real_client(name, *args, **kwargs)

    session.client = cast(Any, client)
    return session


def _run(
    args: list[str],
    fake_lambda: MagicMock,
    user_input: str | None = None,
    fake_tagging: MagicMock | None = None,
):
    if fake_tagging is None:
        fake_tagging = _fake_tagging([FUNCTION_NAME])
    with patch(
        "cli.build_session",
        return_value=_session(fake_lambda, fake_tagging),
    ):
        return CliRunner().invoke(cli, ["--quiet", *args], input=user_input)


def _payload(fake_lambda: MagicMock, call_index: int) -> dict:
    return json.loads(fake_lambda.invoke.call_args_list[call_index].kwargs["Payload"])


@mock_aws
def test_confirming_with_enter_applies_after_dry_run():
    fake = _fake_lambda(
        [_invoke_response(_report([_change()])), _invoke_response(_report([_change()]))]
    )

    result = _run(["manual-update", "publisher", "OLD", "NEW"], fake, user_input="\n")

    assert result.exit_code == 0, result.exception
    assert fake.invoke.call_count == 2
    assert _payload(fake, 0)["dryRun"] is True
    assert _payload(fake, 1)["dryRun"] is False
    assert _payload(fake, 0)["searchParams"] == {"publisher": "OLD"}


@mock_aws
def test_declining_aborts_before_apply():
    fake = _fake_lambda([_invoke_response(_report([_change()]))])

    result = _run(["manual-update", "publisher", "OLD", "NEW"], fake, user_input="n\n")

    assert result.exit_code == 1
    assert fake.invoke.call_count == 1


@mock_aws
def test_yes_flag_skips_confirmation():
    fake = _fake_lambda(
        [_invoke_response(_report([_change()])), _invoke_response(_report([_change()]))]
    )

    result = _run(["manual-update", "publisher", "OLD", "NEW", "--yes"], fake)

    assert result.exit_code == 0, result.exception
    assert fake.invoke.call_count == 2


@mock_aws
def test_dry_run_only_never_applies():
    fake = _fake_lambda([_invoke_response(_report([_change()]))])

    result = _run(["manual-update", "publisher", "OLD", "NEW", "--dry-run-only"], fake)

    assert result.exit_code == 0, result.exception
    assert fake.invoke.call_count == 1


@mock_aws
def test_no_changes_short_circuits_without_prompt():
    fake = _fake_lambda([_invoke_response(_report([]))])

    result = _run(["manual-update", "publisher", "OLD", "NEW"], fake)

    assert result.exit_code == 0, result.exception
    assert fake.invoke.call_count == 1
    assert "No changes" in result.output


@mock_aws
def test_contributor_affiliation_derives_unit_from_uri():
    fake = _fake_lambda([_invoke_response(_report([]))])

    result = _run(
        [
            "manual-update",
            "contributor-affiliation",
            "https://api.nva.unit.no/cristin/organization/209.1.0.0",
            "https://api.nva.unit.no/cristin/organization/209.3.4.0",
        ],
        fake,
    )

    assert result.exit_code == 0, result.exception
    payload = _payload(fake, 0)
    assert payload["type"] == "CONTRIBUTOR_AFFILIATION"
    assert payload["searchParams"] == {"unit": "209.1.0.0"}


@mock_aws
def test_search_flag_overrides_default_search_param():
    fake = _fake_lambda([_invoke_response(_report([]))])

    result = _run(
        [
            "manual-update",
            "publisher",
            "OLD",
            "NEW",
            "--search",
            "publisher=OTHER",
            "--search",
            "unit=209.0.0.0",
        ],
        fake,
    )

    assert result.exit_code == 0, result.exception
    assert _payload(fake, 0)["searchParams"] == {
        "publisher": "OTHER",
        "unit": "209.0.0.0",
    }


@mock_aws
def test_unconfirmed_publisher_sets_query_and_default_comparator():
    fake = _fake_lambda([_invoke_response(_report([]))])

    result = _run(
        ["manual-update", "unconfirmed-publisher", "Some name", "PUBLISHER-ID"], fake
    )

    assert result.exit_code == 0, result.exception
    payload = _payload(fake, 0)
    assert payload["searchParams"] == {
        "publisher": "Some name",
        "query": "UnconfirmedPublisher",
    }
    assert payload["comparator"] == "CONTAINS"


@mock_aws
def test_generic_run_requires_search_parameter():
    fake = _fake_lambda([])

    result = _run(
        ["manual-update", "run", "--type", "PUBLISHER", "--old", "OLD", "--new", "NEW"],
        fake,
    )

    assert result.exit_code != 0
    assert "search" in result.output.lower()
    assert fake.invoke.call_count == 0


@mock_aws
def test_run_again_message_shown_when_results_still_pending():
    fake = _fake_lambda(
        [
            _invoke_response(_report([_change()])),
            _invoke_response(
                _report([_change()], limitReached=True, totalHits=100, hitsReturned=1)
            ),
        ]
    )

    result = _run(["manual-update", "publisher", "OLD", "NEW", "--yes"], fake)

    assert result.exit_code == 0, result.exception
    assert "run again" in " ".join(result.output.split())


@mock_aws
def test_no_run_again_message_when_all_results_were_processed():
    fake = _fake_lambda(
        [
            _invoke_response(_report([_change()])),
            _invoke_response(
                _report(
                    [_change()],
                    limitReached=True,
                    limit=1,
                    totalHits=1,
                    hitsReturned=1,
                )
            ),
        ]
    )

    result = _run(["manual-update", "publisher", "OLD", "NEW", "--yes"], fake)

    assert result.exit_code == 0, result.exception
    assert "run again" not in " ".join(result.output.split())


@mock_aws
def test_summary_hides_defaults():
    fake = _fake_lambda([_invoke_response(_report([]))])

    result = _run(["manual-update", "publisher", "OLD", "NEW"], fake)

    assert result.exit_code == 0, result.exception
    assert "Limit" not in result.output
    assert "Page size" not in result.output
    assert "Resources fetched" not in result.output


@mock_aws
def test_summary_shows_overridden_and_relevant_fields():
    fake = _fake_lambda(
        [
            _invoke_response(
                _report([], totalHits=10, resourcesFetched=7, pageSize=50, limit=5)
            )
        ]
    )

    result = _run(
        [
            "manual-update",
            "publisher",
            "OLD",
            "NEW",
            "--limit",
            "5",
            "--page-size",
            "50",
        ],
        fake,
    )

    assert result.exit_code == 0, result.exception
    assert "Resources fetched" in result.output
    assert "Limit" in result.output
    assert "Page size" in result.output


def _changes(count: int) -> list:
    return [_change(f"id-{index}") for index in range(count)]


@mock_aws
def test_many_changes_are_truncated_in_display(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    fake = _fake_lambda([_invoke_response(_report(_changes(12)))])

    result = _run(["manual-update", "publisher", "OLD", "NEW", "--dry-run-only"], fake)

    assert result.exit_code == 0, result.exception
    output = " ".join(result.output.split())
    assert "… 2 more …" in output
    assert "id-0" in output
    assert "id-11" in output
    assert "id-6" not in output


@mock_aws
def test_apply_always_writes_full_change_log(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    fake = _fake_lambda(
        [_invoke_response(_report(_changes(3))), _invoke_response(_report(_changes(3)))]
    )

    result = _run(["manual-update", "publisher", "OLD", "NEW", "--yes"], fake)

    assert result.exit_code == 0, result.exception
    logs = list((tmp_path / "manual-update-logs").glob("*-applied.json"))
    assert len(logs) == 1
    logged = json.loads(logs[0].read_text())
    assert [change["identifier"] for change in logged["changes"]] == [
        "id-0",
        "id-1",
        "id-2",
    ]


@mock_aws
def test_small_dry_run_writes_no_log(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    fake = _fake_lambda([_invoke_response(_report(_changes(3)))])

    result = _run(["manual-update", "publisher", "OLD", "NEW", "--dry-run-only"], fake)

    assert result.exit_code == 0, result.exception
    assert not (tmp_path / "manual-update-logs").exists()


@mock_aws
def test_large_dry_run_writes_plan_log(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    fake = _fake_lambda([_invoke_response(_report(_changes(12)))])

    result = _run(["manual-update", "publisher", "OLD", "NEW", "--dry-run-only"], fake)

    assert result.exit_code == 0, result.exception
    logs = list((tmp_path / "manual-update-logs").glob("*-dry-run.json"))
    assert len(logs) == 1
    assert len(json.loads(logs[0].read_text())["changes"]) == 12


@mock_aws
def test_missing_lambda_reports_clean_error():
    result = _run(
        ["manual-update", "publisher", "OLD", "NEW"],
        _fake_lambda([]),
        fake_tagging=_fake_tagging([]),
    )

    assert result.exit_code != 0
    assert "No Lambda function" in result.output
