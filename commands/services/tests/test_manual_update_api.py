import io
import json
from typing import Any, cast
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

from commands.services.manual_update_api import (
    Comparator,
    ManualUpdateError,
    ManualUpdateRequest,
    ManualUpdateService,
    ManualUpdateType,
)

FUNCTION_NAME = "master-pipelines-NvaPubli-ManuallyUpdatePublicatio-NFn4HKTz93o7"


def _arn(function_name: str) -> str:
    return f"arn:aws:lambda:eu-west-1:123456789012:function:{function_name}"


def _request(
    comparator: Comparator | None = None,
    limit: int | None = None,
    page_size: int | None = None,
) -> ManualUpdateRequest:
    return ManualUpdateRequest(
        type=ManualUpdateType.PUBLISHER,
        old_value="OLD",
        new_value="NEW",
        search_params={"publisher": "OLD"},
        comparator=comparator,
        limit=limit,
        page_size=page_size,
    )


def _invoke_response(
    report: dict | None = None,
    function_error: str | None = None,
    raw_body: str | None = None,
) -> dict:
    body = raw_body if raw_body is not None else json.dumps(report or {})
    response: dict = {"Payload": io.BytesIO(body.encode("utf-8"))}
    if function_error:
        response["FunctionError"] = function_error
    return response


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


def test_to_payload_always_includes_dry_run_and_omits_unset_fields():
    payload = _request().to_payload(dry_run=True)

    assert payload == {
        "type": "PUBLISHER",
        "oldValue": "OLD",
        "newValue": "NEW",
        "searchParams": {"publisher": "OLD"},
        "dryRun": True,
    }


def test_to_payload_includes_optional_fields_when_set():
    payload = _request(
        comparator=Comparator.CONTAINS, limit=5, page_size=50
    ).to_payload(dry_run=False)

    assert payload["dryRun"] is False
    assert payload["comparator"] == "CONTAINS"
    assert payload["limit"] == 5
    assert payload["pageSize"] == 50


@mock_aws
def test_dry_run_invokes_synchronously_and_returns_report():
    report = {"resourcesChanged": 2, "changes": []}
    fake = _fake_lambda([_invoke_response(report)])
    service = ManualUpdateService(
        session=_session(fake, _fake_tagging([FUNCTION_NAME]))
    )

    result = service.dry_run(_request())

    assert result == report
    invoke_kwargs = fake.invoke.call_args.kwargs
    assert invoke_kwargs["FunctionName"] == FUNCTION_NAME
    assert invoke_kwargs["InvocationType"] == "RequestResponse"
    assert json.loads(invoke_kwargs["Payload"])["dryRun"] is True


@mock_aws
def test_apply_sends_dry_run_false():
    fake = _fake_lambda([_invoke_response({"changes": []})])
    service = ManualUpdateService(
        session=_session(fake, _fake_tagging([FUNCTION_NAME]))
    )

    service.apply(_request())

    assert json.loads(fake.invoke.call_args.kwargs["Payload"])["dryRun"] is False


@mock_aws
def test_raises_when_no_function_found():
    service = ManualUpdateService(session=_session(_fake_lambda([]), _fake_tagging([])))

    with pytest.raises(ManualUpdateError, match="No Lambda function"):
        service.dry_run(_request())


@mock_aws
def test_raises_when_multiple_functions_match():
    tagging = _fake_tagging([FUNCTION_NAME, f"{FUNCTION_NAME}-2"])
    service = ManualUpdateService(session=_session(_fake_lambda([]), tagging))

    with pytest.raises(ManualUpdateError, match="Multiple Lambda functions"):
        service.dry_run(_request())


@mock_aws
def test_raises_on_function_error_with_lambda_message():
    error_body = json.dumps(
        {
            "errorType": "IllegalArgumentException",
            "errorMessage": "Field 'dryRun' is required",
        }
    )
    fake = _fake_lambda(
        [_invoke_response(raw_body=error_body, function_error="Unhandled")]
    )
    service = ManualUpdateService(
        session=_session(fake, _fake_tagging([FUNCTION_NAME]))
    )

    with pytest.raises(ManualUpdateError, match="Field 'dryRun' is required"):
        service.dry_run(_request())


@mock_aws
def test_function_name_resolved_once_and_reused():
    fake = _fake_lambda(
        [_invoke_response({"changes": []}), _invoke_response({"changes": []})]
    )
    tagging = _fake_tagging([FUNCTION_NAME])
    service = ManualUpdateService(session=_session(fake, tagging))

    service.dry_run(_request())
    service.apply(_request())

    assert tagging.get_paginator.call_count == 1
