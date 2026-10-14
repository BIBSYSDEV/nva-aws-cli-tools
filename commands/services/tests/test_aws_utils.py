import boto3
import pytest
from moto import mock_aws

from commands.services.aws_utils import find_table_name


def _create_table(name: str) -> None:
    boto3.client("dynamodb").create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "identifier", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "identifier", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )


@mock_aws
def test_find_table_name_matches_substring():
    _create_table("nva-approvals-master-pipelines-NvaHandleService")
    _create_table("nva-customers-something")

    assert find_table_name(boto3.Session(), "nva-approvals-") == (
        "nva-approvals-master-pipelines-NvaHandleService"
    )


@mock_aws
def test_find_table_name_fails_when_nothing_matches():
    _create_table("nva-customers-something")

    with pytest.raises(ValueError, match="No DynamoDB table found"):
        find_table_name(boto3.Session(), "nva-approvals-")


@mock_aws
def test_find_table_name_fails_when_several_tables_match_without_a_terminal(
    monkeypatch,
):
    _create_table("nva-approvals-stack-one")
    _create_table("nva-approvals-stack-two")
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    with pytest.raises(ValueError, match="Several DynamoDB tables"):
        find_table_name(boto3.Session(), "nva-approvals-")


@mock_aws
def test_find_table_name_lets_user_choose_when_several_tables_match(monkeypatch):
    _create_table("nva-customers-master-pipelines-NvaIdentityService")
    _create_table("backup-duplicate-nva-customers-master-pipelines")
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("click.prompt", lambda *args, **kwargs: 2)

    assert find_table_name(boto3.Session(), "nva-customers") == (
        "nva-customers-master-pipelines-NvaIdentityService"
    )


@mock_aws
def test_find_table_name_lists_candidates_sorted_when_prompting(monkeypatch, capsys):
    _create_table("nva-customers-stack-two")
    _create_table("nva-customers-stack-one")
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("click.prompt", lambda *args, **kwargs: 1)

    find_table_name(boto3.Session(), "nva-customers")

    printed_lines = capsys.readouterr().out.splitlines()
    assert printed_lines[1:] == [
        "  1. nva-customers-stack-one",
        "  2. nva-customers-stack-two",
    ]
