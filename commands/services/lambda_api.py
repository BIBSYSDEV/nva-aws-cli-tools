import json
import logging

import boto3

from commands.services.aws_utils import get_account_alias

logger = logging.getLogger(__name__)


def invoke_function(
    session: boto3.Session, function_name: str, payload: str | None = None
) -> dict:
    args: dict = {"FunctionName": function_name, "InvocationType": "Event"}
    if payload:
        args["Payload"] = payload
    return session.client("lambda").invoke(**args)


def find_function_name(session: boto3.Session, name_partial: str) -> list[str]:
    client = session.client("lambda")
    matching_functions = []
    for page in client.get_paginator("list_functions").paginate():
        for function in page["Functions"]:
            if name_partial.lower() in function["FunctionName"].lower():
                matching_functions.append(function["FunctionName"])
    return matching_functions


def cleanup_old_versions(session: boto3.Session, delete: bool) -> None:
    client = session.client("lambda")
    functions_paginator = client.get_paginator("list_functions")
    version_paginator = client.get_paginator("list_versions_by_function")

    for function_page in functions_paginator.paginate():
        for function in function_page["Functions"]:
            function_name = function["FunctionName"]
            aliases = client.list_aliases(FunctionName=function_name)
            alias_versions = {alias["FunctionVersion"] for alias in aliases["Aliases"]}
            for version_page in version_paginator.paginate(FunctionName=function_name):
                for version in version_page["Versions"]:
                    arn = version["FunctionArn"]
                    if (
                        version["Version"] != function["Version"]
                        and version["Version"] not in alias_versions
                    ):
                        logger.info("  🥊 %s", arn)
                        if delete:
                            client.delete_function(FunctionName=arn)
                    else:
                        logger.info("  💚 %s", arn)


def concurrency_report(session: boto3.Session) -> None:
    client = session.client("lambda")
    functions = []

    for page in client.get_paginator("list_functions").paginate():
        for function in page["Functions"]:
            function_name = function["FunctionName"]
            try:
                concurrency = client.get_function_concurrency(
                    FunctionName=function_name
                )
                reserved_concurrency = concurrency.get(
                    "ReservedConcurrentExecutions", None
                )
            except client.exceptions.ResourceNotFoundException:
                reserved_concurrency = None
            functions.append(
                {
                    "FunctionName": function_name,
                    "ReservedConcurrency": reserved_concurrency,
                }
            )

    functions.sort(
        key=lambda function: (
            function["ReservedConcurrency"]
            if function["ReservedConcurrency"] is not None
            else -1
        ),
        reverse=True,
    )

    account_alias = get_account_alias(session)
    with open(f"{account_alias}_lambda_concurrency.json", "w") as f:
        json.dump(functions, f, indent=4)
