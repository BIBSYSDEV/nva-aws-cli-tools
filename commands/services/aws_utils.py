# aws_utils.py
import boto3
import json
import click
import subprocess
import json
import os
from deepdiff import DeepDiff
import re

def get_account_alias(profile: str = None) -> str:
    # Create a default Boto3 session
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()

    # Create an IAM client
    iam = session.client("iam")

    # Get the account alias
    account_aliases = iam.list_account_aliases()["AccountAliases"]

    # Return the first account alias or None if the list is empty
    return account_aliases[0] if account_aliases else None


def prettify(object) -> str:
    return json.dumps(
        object, indent=2, sort_keys=False, default=str, ensure_ascii=False
    )


def edit_and_diff(item, update_callback):
    item.pop("@context", None)

    folder_name = "publication_data"
    os.makedirs(folder_name, exist_ok=True)

    file_name = os.path.join(folder_name, f"{item['identifier']}.json")

    with open(file_name, "w") as file:
        file.write(prettify(item))

    try:
        subprocess.run(["code", "--new-window", "--wait", file_name])
    except FileNotFoundError:
        click.echo("Error: The specified editor could not be found.")
        return

    with open(file_name, "r") as file:
        updated_publication = json.load(file)

    diff = DeepDiff(item, updated_publication, ignore_order=True)

    if diff:
        click.echo("Changes detected in the publication:")
        click.echo(diff.pretty())

        if click.confirm("Do you want to save these changes?", default=False):
            update_callback(updated_publication)
            click.echo("Changes saved successfully.")
        else:
            click.echo("Changes were not saved.")
    else:
        click.echo("No changes detected. Nothing to save.")


def extract_publication_identifier(url: str) -> str:
    match = re.search(r"/([0-9a-fA-F\-]{49}|[0-9a-fA-F\-]{36})$", url)
    if match:
        return match.group(1)
    raise ValueError(f"No valid publication_identifier found in the URL: {url}")