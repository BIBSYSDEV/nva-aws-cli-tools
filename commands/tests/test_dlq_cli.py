import json

import boto3
from click.testing import CliRunner
from moto import mock_aws

from cli import cli


def _seed_queue(name: str, bodies: list[str]) -> str:
    sqs = boto3.client("sqs")
    queue_url = sqs.create_queue(QueueName=name)["QueueUrl"]
    for body in bodies:
        sqs.send_message(QueueUrl=queue_url, MessageBody=body)
    return queue_url


@mock_aws
def test_dlq_read_groups_messages_by_body_with_counts():
    queue_url = _seed_queue(
        "nva-test-dlq",
        ["Failed to migrate doc xyz", "Failed to migrate doc xyz", "Other error"],
    )

    result = CliRunner().invoke(cli, ["--quiet", "dlq", "read", "--queue", queue_url])

    assert result.exit_code == 0, result.exception
    summary = json.loads(result.output)
    assert summary["by_body"]["Failed to migrate doc xyz"]["count"] == 2
    assert summary["by_body"]["Other error"]["count"] == 1


@mock_aws
def test_dlq_purge_dry_run_reports_matches_and_does_not_delete():
    queue_url = _seed_queue(
        "nva-test-dlq",
        ["DocumentNotFound: 1", "DocumentNotFound: 2", "OtherError: 3"],
    )

    result = CliRunner().invoke(
        cli, ["--quiet", "dlq", "purge", "--queue", queue_url, "--prefix", "DocumentNotFound", "--dry-run"]
    )

    assert result.exit_code == 0, result.exception
    report = json.loads(result.output)
    assert report["matched_count"] == 2

    attributes = boto3.client("sqs").get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    visible = int(attributes["ApproximateNumberOfMessages"])
    in_flight = int(attributes["ApproximateNumberOfMessagesNotVisible"])
    assert visible + in_flight == 3
