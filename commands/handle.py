import click
import json
import os
import shutil
import boto3
from commands.services.aws_utils import get_account_alias
from commands.services.handle_task_writer import HandleTaskWriterService
from commands.services.handle_task_executor import HandleTaskExecutorService


def get_application_domain(profile):
    session = boto3.Session(profile_name=profile)
    ssm = session.client("ssm")
    response = ssm.get_parameter(Name="/NVA/ApplicationDomain")
    return response["Parameter"]["Value"]


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
    "-i",
    "--input-file",
    required=True,
    help="Input JSON file path with handle data. e.g. additional_identifier_handles.json",
)
@click.option(
    "-o",
    "--output-folder",
    required=False,
    help="Output folder path. e.g. sikt-nva-sandbox-handle-tasks",
)
@click.option(
    "--prefixes",
    default="11250,11250.1,1956,10642,20.500.12199,20.500.12242",
    help="Comma-separated list of controlled handle prefixes",
)
def prepare(profile: str, input_file: str, output_folder: str, controlled_prefixes: str) -> None:
    batch_size = 700

    if not output_folder:
        output_folder = f"{get_account_alias(profile)}-handle-tasks"

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    application_domain = get_application_domain(profile)
    prefixes_list = [prefix.strip() for prefix in controlled_prefixes.split(",")]
    task_writer = HandleTaskWriterService(application_domain, prefixes_list)

    with open(input_file, 'r', encoding='utf-8') as f:
        handle_data = json.load(f)

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

    for handle_value, handle_info in handle_data.items():
        processed_count += 1

        if processed_count % 10000 == 0:
            print(f"Processed {processed_count} handles...")

        tasks = task_writer.process_handle_from_json(handle_value, handle_info)

        for task in tasks:
            accumulated_tasks.append(task)
            task_count += 1

            if len(accumulated_tasks) >= batch_size:
                write_task_file()

    write_task_file()

    print(f"\nProcessed {processed_count} handles")
    print(f"Found {task_count} tasks to process")
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
    help="Input folder path. e.g. sikt-nva-sandbox-handle-tasks",
)
@click.option(
    "--max-workers",
    default=5,
    type=int,
    help="Maximum number of concurrent threads for network requests",
)
def create(profile: str, input_folder: str, max_workers: int) -> None:
    complete_folder = os.path.join(input_folder, "complete")
    os.makedirs(complete_folder, exist_ok=True)

    executor = HandleTaskExecutorService(profile, input_folder, max_workers)

    for batch_file in os.listdir(input_folder):
        file_path = os.path.join(input_folder, batch_file)
        if os.path.isfile(file_path) and batch_file.endswith(".jsonl"):
            print(f"Processing {batch_file}...")
            with open(file_path, "r") as infile:
                batch = [json.loads(line) for line in infile]
                executor.execute_create(batch)

            # Move the file to the 'complete' folder after processing
            new_file_path = os.path.join(complete_folder, batch_file)
            shutil.move(file_path, new_file_path)
            print(f"Completed {batch_file}")


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
    help="Input folder path. e.g. sikt-nva-sandbox-handle-tasks",
)
@click.option(
    "--max-workers",
    default=5,
    type=int,
    help="Maximum number of concurrent threads for network requests",
)
def update(profile: str, input_folder: str, max_workers: int) -> None:
    complete_folder = os.path.join(input_folder, "complete")
    os.makedirs(complete_folder, exist_ok=True)

    executor = HandleTaskExecutorService(profile, input_folder, max_workers)

    for batch_file in os.listdir(input_folder):
        file_path = os.path.join(input_folder, batch_file)
        if os.path.isfile(file_path) and batch_file.endswith(".jsonl"):
            print(f"Processing {batch_file}...")
            with open(file_path, "r") as infile:
                batch = [json.loads(line) for line in infile]
                executor.execute_update(batch)

            # Move the file to the 'complete' folder after processing
            new_file_path = os.path.join(complete_folder, batch_file)
            shutil.move(file_path, new_file_path)
            print(f"Completed {batch_file}")
