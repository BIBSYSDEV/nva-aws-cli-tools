import boto3


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
