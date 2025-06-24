import boto3
import json

from commands.services.aws_utils import get_account_alias


class LambdaService:
    def __init__(self, profile):
        self.profile = profile
        pass

    def delete_old_versions(self, delete):
        session = boto3.Session(profile_name=self.profile)
        client = session.client("lambda")

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
                            print("  🥊 {}".format(arn))
                            if delete:
                                client.delete_function(FunctionName=arn)
                        else:
                            print("  💚 {}".format(arn))

    def concurrency(self):
        session = boto3.Session(profile_name=self.profile)
        client = session.client("lambda")

        functions = []

        paginator = client.get_paginator("list_functions")

        for page in paginator.paginate():
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
