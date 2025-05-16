import click
from commands.services.lambda_api import LambdaService


@click.group()
def awslambda():
    pass


@awslambda.command(help="Delete old versions of AWS Lambda functions.")
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.option("--delete", is_flag=True, default=True, help="Delete old versions.")
def delete_old_versions(profile: str, delete: bool) -> None:
    LambdaService(profile).delete_old_versions(delete)

@awslambda.command(help="Generate concurrency report for AWS Lambda functions.")
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
def concurrency(profile: str) -> None:
    LambdaService(profile).concurrency()