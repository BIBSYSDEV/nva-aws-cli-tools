# aws_utils.py
import boto3
import json


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
    return json.dumps(object, indent=2, sort_keys=True, default=str, ensure_ascii=False)
