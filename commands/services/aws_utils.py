# aws_utils.py
import json
import os
import re
import subprocess

import boto3
import click
from deepdiff import DeepDiff


def build_session(profile: str | None = None) -> boto3.Session:
    return boto3.Session(profile_name=profile) if profile else boto3.Session()


def get_ssm_parameter(session: boto3.Session, name: str) -> str:
    return session.client("ssm").get_parameter(Name=name)["Parameter"]["Value"]


def get_account_alias(session: boto3.Session) -> str | None:
    account_aliases = session.client("iam").list_account_aliases()["AccountAliases"]
    return account_aliases[0] if account_aliases else None


def find_table_name(session: boto3.Session, name_substring: str) -> str:
    matching_names = [
        table_name
        for table_name in _list_all_table_names(session)
        if name_substring in table_name
    ]
    if not matching_names:
        raise ValueError(f"No DynamoDB table found containing {name_substring!r}")
    if len(matching_names) > 1:
        raise ValueError(
            f"Several DynamoDB tables contain {name_substring!r}: "
            f"{', '.join(sorted(matching_names))}. Use a more specific name."
        )
    return matching_names[0]


def _list_all_table_names(session: boto3.Session) -> list[str]:
    paginator = session.client("dynamodb").get_paginator("list_tables")
    return [
        table_name for page in paginator.paginate() for table_name in page["TableNames"]
    ]


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
        subprocess.run(["code", "--new-window", "--wait", file_name], check=True)
    except FileNotFoundError:
        click.echo("Error: The specified editor could not be found.")
        return
    except subprocess.CalledProcessError as error:
        click.echo(f"Error: The editor exited with an error: {error}")
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
