import boto3
from click.testing import CliRunner
from moto import mock_aws

from cli import cli


def _create_queue(name: str, message_count: int = 0) -> str:
    sqs = boto3.client("sqs")
    queue_url = sqs.create_queue(QueueName=name)["QueueUrl"]
    for index in range(message_count):
        sqs.send_message(QueueUrl=queue_url, MessageBody=f"message-{index}")
    return queue_url


def _table_row(output: str, queue_name: str) -> list[str]:
    matching_lines = [line for line in output.splitlines() if queue_name in line]
    assert matching_lines, f"No table row for '{queue_name}' in output:\n{output}"
    cells = [cell.strip() for cell in matching_lines[0].split("│")]
    return [cell for cell in cells if cell]


@mock_aws
def test_list_shows_only_queues_with_messages_by_default():
    _create_queue("busy-queue", message_count=3)
    _create_queue("empty-queue")

    result = CliRunner().invoke(cli, ["--quiet", "sqs", "list"])

    assert result.exit_code == 0, result.exception
    assert "busy-queue" in result.output
    assert "empty-queue" not in result.output
    assert "1 empty hidden" in result.output

    row = _table_row(result.output, "busy-queue")
    assert row == ["busy-queue", "3", "0"]


@mock_aws
def test_list_include_empty_shows_all_queues():
    _create_queue("busy-queue", message_count=2)
    _create_queue("empty-queue")

    result = CliRunner().invoke(cli, ["--quiet", "sqs", "list", "--include-empty"])

    assert result.exit_code == 0, result.exception
    assert _table_row(result.output, "busy-queue") == ["busy-queue", "2", "0"]
    assert _table_row(result.output, "empty-queue") == ["empty-queue", "0", "0"]
    assert "Total: 2 queue(s)" in result.output


@mock_aws
def test_list_reports_messages_in_flight():
    queue_url = _create_queue("inflight-queue", message_count=2)
    boto3.client("sqs").receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)

    result = CliRunner().invoke(cli, ["--quiet", "sqs", "list"])

    assert result.exit_code == 0, result.exception
    assert _table_row(result.output, "inflight-queue") == ["inflight-queue", "1", "1"]


@mock_aws
def test_list_filter_matches_queue_names():
    _create_queue("orders-dlq", message_count=1)
    _create_queue("payments-dlq", message_count=1)

    result = CliRunner().invoke(cli, ["--quiet", "sqs", "list", "--filter", "orders"])

    assert result.exit_code == 0, result.exception
    assert "orders-dlq" in result.output
    assert "payments-dlq" not in result.output
    assert "Total: 1 queue(s)" in result.output


@mock_aws
def test_list_reports_when_all_queues_are_empty():
    _create_queue("empty-queue")

    result = CliRunner().invoke(cli, ["--quiet", "sqs", "list"])

    assert result.exit_code == 0, result.exception
    assert "No queues with messages" in result.output
    assert "--include-empty" in result.output


@mock_aws
def test_list_reports_when_no_queues_exist():
    result = CliRunner().invoke(cli, ["--quiet", "sqs", "list"])

    assert result.exit_code == 0, result.exception
    assert "No queues found" in result.output
