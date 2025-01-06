import click
import json
import os
import shutil
from multiprocessing import Pool
from boto3.dynamodb.conditions import Key
from commands.services.handle_task_writer import HandleTaskWriterService
from commands.services.handle_task_executor import  HandleTaskExecutorService
from commands.services.dynamodb_export import DynamodbExport

@click.group()
def handle():
    pass

@handle.command()
@click.option('--profile', envvar='AWS_PROFILE', default='default', help='The AWS profile to use. e.q LimitedAdmin-000123456789')
@click.option('-c', '--customer', required=True, help='Customer UUID')
@click.option('-r', '--resource-owner', required=True, help='Resource owner ID')
@click.option('-o', '--output-folder', required=False, help='Output folder path')
def prepare(profile, customer, resource_owner, output_folder):
    table_pattern = '^nva-resources-master-pipelines-NvaPublicationApiPipeline-.*-nva-publication-api$'
    condition = Key('PK0').eq(f'Resource:{customer}:{resource_owner}')
    batch_size = 700

    if not output_folder:
        output_folder = f'{get_account_alias(profile)}-resources-{resource_owner}-handle-tasks'

    # Create output folder if not exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    action_counts = {}

    def process_batch(batch, batch_counter):
        with open(f'{output_folder}/batch_{batch_counter}.jsonl', 'w') as outfile:
            for data in batch:
                task = HandleTaskWriterService().process_item(data)
                action = task.get('action')
                action_counts[action] = action_counts.get(action, 0) + 1
                json.dump(task, outfile)
                outfile.write('\n')

    DynamodbExport(profile, table_pattern, condition, batch_size).process(process_batch)

    print("Action counts: ", action_counts)
    print(f"Customer: {customer}")
    print(f"Output Folder: {output_folder}")


@handle.command()
@click.option('--profile', envvar='AWS_PROFILE', default='default', help='The AWS profile to use.')
@click.option('-i', '--input-folder', required=True, help='Input folder path')
def execute(profile, input_folder):
    complete_folder = os.path.join(input_folder, 'complete')
    os.makedirs(complete_folder, exist_ok=True)

    for batch_file in os.listdir(input_folder):
        file_path = os.path.join(input_folder, batch_file)
        if os.path.isfile(file_path):
            with open(file_path, 'r') as infile:
                batch = [json.loads(line) for line in infile]
                HandleTaskExecutorService(profile).execute(batch)
            
            # Move the file to the 'complete' folder after processing
            new_file_path = os.path.join(complete_folder, batch_file)
            shutil.move(file_path, new_file_path)
