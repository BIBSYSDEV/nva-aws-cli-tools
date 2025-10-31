import click
import json
import os
import shutil
from boto3.dynamodb.conditions import Attr
from commands.services.aws_utils import get_account_alias
from commands.services.handle_task_writer import HandleTaskWriterService
from commands.services.handle_task_executor import HandleTaskExecutorService
from commands.services.dynamodb_publications import DynamodbPublications


@click.group()
def handle():
    pass


@handle.command()
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.option(
    "-o",
    "--output-folder",
    required=False,
    help="Output folder path. e.g. sikt-nva-sandbox-handle-tasks",
)
def prepare(profile: str, output_folder: str) -> None:
    table_pattern = "^nva-resources-master-pipelines-NvaPublicationApiPipeline-.*-nva-publication-api$"
    batch_size = 700
    condition = Attr("PK2").begins_with("Customer:") & Attr("SK2").begins_with(
        "a:Resource:"
    )

    if not output_folder:
        output_folder = f"{get_account_alias(profile)}-handle-tasks"

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    task_writer = HandleTaskWriterService(profile)
    
    processed_count = 0
    task_count = 0
    accumulated_tasks = []
    file_counter = 0

    def write_task_file():
        nonlocal file_counter, accumulated_tasks
        if accumulated_tasks:
            filename = f"{output_folder}/batch_{file_counter}.jsonl"
            with open(filename, "w") as outfile:
                for task in accumulated_tasks:
                    json.dump(task, outfile)
                    outfile.write("\n")
            print(f"Wrote {len(accumulated_tasks)} tasks to batch_{file_counter}.jsonl")
            file_counter += 1
            accumulated_tasks = []

    def process_batch(batch, _batch_counter):
        nonlocal processed_count, task_count, accumulated_tasks

        for publication in batch:
            processed_count += 1
            tasks = task_writer.process_item(publication)

            for task in tasks:
                accumulated_tasks.append(task)
                task_count += 1

                if len(accumulated_tasks) >= batch_size:
                    write_task_file()

    DynamodbPublications(profile, table_pattern).process_scan(
        condition, batch_size, process_batch
    )

    write_task_file()

    print(f"\nProcessed {processed_count} publications")
    print(f"Found {task_count} handles to migrate")
    print(f"Controlled prefixes: {task_writer.controlled_prefixes}")
    print(f"Output Folder: {output_folder}")


@handle.command()
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.option(
    "-i",
    "--input-folder",
    required=True,
    help="Input folder path. e.g. sikt-nva-sandbox-resources-ntnu@194.0.0.0-handle-tasks",
)
def execute(profile: str, input_folder: str) -> None:
    complete_folder = os.path.join(input_folder, "complete")
    os.makedirs(complete_folder, exist_ok=True)

    for batch_file in os.listdir(input_folder):
        file_path = os.path.join(input_folder, batch_file)
        if os.path.isfile(file_path) and batch_file.endswith(".jsonl"):
            with open(file_path, "r") as infile:
                batch = [json.loads(line) for line in infile]
                HandleTaskExecutorService(profile, input_folder).execute(batch)

            # Move the file to the 'complete' folder after processing
            new_file_path = os.path.join(complete_folder, batch_file)
            shutil.move(file_path, new_file_path)
