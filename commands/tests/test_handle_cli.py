import base64
import json
import zlib

import boto3
from click.testing import CliRunner
from moto import mock_aws

from cli import cli

PUBLICATIONS_TABLE = (
    "nva-resources-master-pipelines-NvaPublicationApiPipeline-FAKE-nva-publication-api"
)


def _create_publications_table() -> None:
    boto3.client("dynamodb").create_table(
        TableName=PUBLICATIONS_TABLE,
        KeySchema=[
            {"AttributeName": "PK0", "KeyType": "HASH"},
            {"AttributeName": "SK0", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK0", "AttributeType": "S"},
            {"AttributeName": "SK0", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )


def _put_publication(customer: str, resource_owner: str, publication: dict) -> None:
    """Store a publication in the format DynamodbPublications expects: zlib-compressed JSON in `data`."""
    deflated = zlib.compress(json.dumps(publication).encode("utf-8"))[2:-4]
    encoded = base64.b64encode(deflated).decode("utf-8")
    boto3.resource("dynamodb").Table(PUBLICATIONS_TABLE).put_item(
        Item={
            "PK0": f"Resource:{customer}:{resource_owner}",
            "SK0": f"Resource:{publication['identifier']}",
            "data": encoded,
        }
    )


@mock_aws
def test_prepare_writes_one_task_per_publication_with_correct_action(tmp_path):
    _create_publications_table()
    _put_publication(
        "cust-1",
        "ntnu@194.0.0.0",
        {
            "identifier": "pub-managed",
            "publication": "https://hdl.handle.net/11250/12345",
        },
    )
    _put_publication(
        "cust-1",
        "ntnu@194.0.0.0",
        {"identifier": "pub-needs-handle"},
    )

    output_folder = tmp_path / "tasks"
    result = CliRunner().invoke(
        cli,
        [
            "--quiet",
            "handle",
            "prepare",
            "--customer",
            "cust-1",
            "--resource-owner",
            "ntnu@194.0.0.0",
            "--output-folder",
            str(output_folder),
        ],
    )

    assert result.exit_code == 0, result.exception
    batch_files = list(output_folder.glob("batch_*.jsonl"))
    assert len(batch_files) == 1

    tasks = [json.loads(line) for line in batch_files[0].read_text().splitlines()]
    actions_by_identifier = {task["identifier"]: task["action"] for task in tasks}
    assert actions_by_identifier == {
        "pub-managed": "nop",
        "pub-needs-handle": "create_new_top",
    }


@mock_aws
def test_prepare_only_processes_publications_for_the_requested_customer(tmp_path):
    _create_publications_table()
    _put_publication("cust-1", "ntnu@194.0.0.0", {"identifier": "in-scope"})
    _put_publication("cust-2", "ntnu@194.0.0.0", {"identifier": "other-customer"})

    output_folder = tmp_path / "tasks"
    result = CliRunner().invoke(
        cli,
        [
            "--quiet",
            "handle",
            "prepare",
            "--customer",
            "cust-1",
            "--resource-owner",
            "ntnu@194.0.0.0",
            "--output-folder",
            str(output_folder),
        ],
    )

    assert result.exit_code == 0, result.exception
    batch_files = list(output_folder.glob("batch_*.jsonl"))
    tasks = [
        json.loads(line) for f in batch_files for line in f.read_text().splitlines()
    ]
    assert [task["identifier"] for task in tasks] == ["in-scope"]
