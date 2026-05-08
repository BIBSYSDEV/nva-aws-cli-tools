import io
import json
import zipfile
from unittest.mock import MagicMock, patch

import boto3
import pytest
from click.testing import CliRunner
from moto import mock_aws

from cli import cli

LAMBDA_TRUST_POLICY = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
)


def _create_function(name: str) -> str:
    role_arn = boto3.client("iam").create_role(
        RoleName=f"{name}-role", AssumeRolePolicyDocument=LAMBDA_TRUST_POLICY
    )["Role"]["Arn"]
    return boto3.client("lambda").create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role=role_arn,
        Handler="handler.handler",
        Code={"ZipFile": _zip_handler("v0")},
    )["FunctionArn"]


def _publish_versions(name: str, count: int) -> None:
    """Publish `count` distinct versions. Each call updates code so AWS records a new version."""
    client = boto3.client("lambda")
    for marker in range(count):
        client.update_function_code(
            FunctionName=name, ZipFile=_zip_handler(f"v{marker + 1}")
        )
        client.publish_version(FunctionName=name)


def _create_alias(name: str, version: str, alias: str = "prod") -> None:
    boto3.client("lambda").create_alias(
        FunctionName=name, Name=alias, FunctionVersion=version
    )


def _set_reserved_concurrency(name: str, value: int) -> None:
    boto3.client("lambda").put_function_concurrency(
        FunctionName=name, ReservedConcurrentExecutions=value
    )


def _list_versions(name: str) -> set[str]:
    response = boto3.client("lambda").list_versions_by_function(FunctionName=name)
    return {item["Version"] for item in response["Versions"]}


def _zip_handler(marker: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "handler.py", f"def handler(event, context): return {marker!r}"
        )
    buffer.seek(0)
    return buffer.read()


@mock_aws
def test_delete_old_versions_skips_current_and_aliased():
    _create_function("my-func")
    _publish_versions("my-func", count=3)
    _create_alias("my-func", version="2")

    result = CliRunner().invoke(cli, ["--quiet", "awslambda", "delete-old-versions"])

    assert result.exit_code == 0, result.exception
    assert _list_versions("my-func") == {"$LATEST", "2"}


@mock_aws
def test_concurrency_writes_functions_sorted_desc_by_reserved_concurrency(
    monkeypatch, tmp_path
):
    boto3.client("iam").create_account_alias(AccountAlias="nva-test")
    monkeypatch.chdir(tmp_path)
    _create_function("func-low")
    _create_function("func-no-reserved")
    _create_function("func-high")
    _set_reserved_concurrency("func-low", 50)
    _set_reserved_concurrency("func-high", 100)

    result = CliRunner().invoke(cli, ["--quiet", "awslambda", "concurrency"])

    assert result.exit_code == 0, result.exception
    report = json.loads((tmp_path / "nva-test_lambda_concurrency.json").read_text())
    assert [item["FunctionName"] for item in report] == [
        "func-high",
        "func-low",
        "func-no-reserved",
    ]
    assert report[2]["ReservedConcurrency"] is None


def _build_session_with_stubbed_lambda(fake_lambda) -> boto3.Session:
    session = boto3.Session()
    real_client = session.client
    session.client = lambda name, *args, **kwargs: (
        fake_lambda if name == "lambda" else real_client(name, *args, **kwargs)
    )
    return session


@pytest.mark.parametrize("payload_arg", [["--body", '{"key":"value"}'], []])
@mock_aws
def test_invoke_calls_lambda_with_resolved_function_name(payload_arg):
    fake_lambda = MagicMock()
    fake_lambda.get_paginator.return_value.paginate = lambda *args, **kwargs: iter(
        [{"Functions": [{"FunctionName": "nva-publication-handler"}]}]
    )

    with patch(
        "cli.build_session",
        return_value=_build_session_with_stubbed_lambda(fake_lambda),
    ):
        result = CliRunner().invoke(
            cli,
            [
                "--quiet",
                "awslambda",
                "invoke",
                "publication-handler",
                "--yes",
                *payload_arg,
            ],
        )

    assert result.exit_code == 0, result.exception
    fake_lambda.invoke.assert_called_once()
    invoke_kwargs = fake_lambda.invoke.call_args.kwargs
    assert invoke_kwargs["FunctionName"] == "nva-publication-handler"
    assert invoke_kwargs["InvocationType"] == "Event"
    if payload_arg:
        assert invoke_kwargs["Payload"] == '{"key":"value"}'
    else:
        assert "Payload" not in invoke_kwargs
