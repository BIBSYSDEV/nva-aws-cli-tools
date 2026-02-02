import click
import json
import logging
import os
import shutil
from boto3.dynamodb.conditions import Key
from commands.utils import AppContext
from commands.services.aws_utils import get_account_alias
from commands.services.handle_task_writer import HandleTaskWriterService
from commands.services.handle_task_executor import HandleTaskExecutorService
from commands.services.dynamodb_publications import DynamodbPublications

logger = logging.getLogger(__name__)


@click.group()
@click.pass_obj
def handle(ctx: AppContext):
    pass


@handle.command()
@click.option(
    "-c",
    "--customer",
    required=True,
    help="Customer UUID. e.g. bb3d0c0c-5065-4623-9b98-5810983c2478",
)
@click.option(
    "-r",
    "--resource-owner",
    required=True,
    help="Resource owner ID. e.g. ntnu@194.0.0.0",
)
@click.option(
    "-o",
    "--output-folder",
    required=False,
    help="Output folder path. e.g. sikt-nva-sandbox-resources-ntnu@194.0.0.0-handle-tasks",
)
@click.option(
    "--prefix",
    required=False,
    help="handle.net prefix. e.g. 20.500.12242 that should be imported to NVA",
)
@click.pass_obj
def prepare(
    ctx: AppContext, customer: str, resource_owner: str, output_folder: str, prefix: str
) -> None:
    table_pattern = "^nva-resources-master-pipelines-NvaPublicationApiPipeline-.*-nva-publication-api$"
    condition = Key("PK0").eq(f"Resource:{customer}:{resource_owner}")
    batch_size = 700

    if not output_folder:
        output_folder = (
            f"{get_account_alias(ctx.profile)}-resources-{resource_owner}-handle-tasks"
        )

    # Create output folder if not exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    action_counts = {}

    def process_batch(batch, batch_counter):
        with open(f"{output_folder}/batch_{batch_counter}.jsonl", "w") as outfile:
            for data in batch:
                task = HandleTaskWriterService().process_item(data, prefix)
                action = task.get("action")
                action_counts[action] = action_counts.get(action, 0) + 1
                json.dump(task, outfile)
                outfile.write("\n")

    DynamodbPublications(ctx.profile, table_pattern).process_query(
        condition, batch_size, process_batch
    )

    logger.info(f"Action counts: {action_counts}")
    logger.info(f"Customer: {customer}")
    logger.info(f"Output Folder: {output_folder}")


@handle.command()
@click.option(
    "-i",
    "--input-folder",
    required=True,
    help="Input folder path. e.g. sikt-nva-sandbox-resources-ntnu@194.0.0.0-handle-tasks",
)
@click.pass_obj
def execute(ctx: AppContext, input_folder: str) -> None:
    complete_folder = os.path.join(input_folder, "complete")
    os.makedirs(complete_folder, exist_ok=True)

    for batch_file in os.listdir(input_folder):
        file_path = os.path.join(input_folder, batch_file)
        if os.path.isfile(file_path) and batch_file.endswith(".jsonl"):
            with open(file_path, "r") as infile:
                batch = [json.loads(line) for line in infile]
                HandleTaskExecutorService(ctx.profile, input_folder).execute(batch)

            # Move the file to the 'complete' folder after processing
            new_file_path = os.path.join(complete_folder, batch_file)
            shutil.move(file_path, new_file_path)
