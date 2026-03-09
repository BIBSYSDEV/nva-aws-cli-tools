import boto3
import json
import logging

from commands.services.aws_utils import get_account_alias

logger = logging.getLogger(__name__)


class LambdaService:
    def __init__(self, profile):
        self.profile = profile
        session = boto3.Session(profile_name=self.profile)
        self.client = session.client("lambda")

    def invoke_function(self, function_name: str, payload: str | None = None) -> dict:
        invoke_args = {
            "FunctionName": function_name,
            "InvocationType": "Event",
        }
        if payload:
            invoke_args["Payload"] = payload

        return self.client.invoke(**invoke_args)

    def find_function_name(self, name_partial: str) -> list[str]:
        matching_functions = []
        paginator = self.client.get_paginator("list_functions")
        for page in paginator.paginate():
            for function in page["Functions"]:
                if name_partial.lower() in function["FunctionName"].lower():
                    matching_functions.append(function["FunctionName"])
        return matching_functions

    def delete_old_versions(self, delete):
        client = self.client

        functions_paginator = client.get_paginator("list_functions")
        version_paginator = client.get_paginator("list_versions_by_function")

        for function_page in functions_paginator.paginate():
            for function in function_page["Functions"]:
                aliases = client.list_aliases(FunctionName=function["FunctionArn"])
                alias_versions = [
                    alias["FunctionVersion"] for alias in aliases["Aliases"]
                ]
                for version_page in version_paginator.paginate(
                    FunctionName=function["FunctionArn"]
                ):
                    for version in version_page["Versions"]:
                        arn = version["FunctionArn"]
                        if (
                            version["Version"] != function["Version"]
                            and version["Version"] not in alias_versions
                        ):
                            logger.info("  🥊 {}".format(arn))
                            if delete:
                                client.delete_function(FunctionName=arn)
                        else:
                            logger.info("  💚 {}".format(arn))

    def concurrency(self):
        functions = []

        paginator = self.client.get_paginator("list_functions")

        for page in paginator.paginate():
            for function in page["Functions"]:
                function_name = function["FunctionName"]
                try:
                    concurrency = self.client.get_function_concurrency(
                        FunctionName=function_name
                    )
                    reserved_concurrency = concurrency.get(
                        "ReservedConcurrentExecutions", None
                    )
                except self.client.exceptions.ResourceNotFoundException:
                    # If a function doesn't have reserved concurrency, AWS will raise a ResourceNotFoundException.
                    reserved_concurrency = None
                functions.append(
                    {
                        "FunctionName": function_name,
                        "ReservedConcurrency": reserved_concurrency,
                    }
                )

        # Sort functions by reserved concurrency in descending order
        functions.sort(
            key=lambda x: x["ReservedConcurrency"]
            if x["ReservedConcurrency"] is not None
            else -1,
            reverse=True,
        )

        # Get account alias
        account_alias = get_account_alias(self.profile)

        # Output to json file
        with open(f"{account_alias}_lambda_concurrency.json", "w") as f:
            json.dump(functions, f, indent=4)
