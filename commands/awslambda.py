import click
from commands.utils import AppContext
from commands.services.lambda_api import LambdaService


@click.group()
@click.pass_obj
def awslambda(ctx: AppContext):
    pass


@awslambda.command(help="Delete old versions of AWS Lambda functions.")
@click.option("--delete", is_flag=True, default=True, help="Delete old versions.")
@click.pass_obj
def delete_old_versions(ctx: AppContext, delete: bool) -> None:
    LambdaService(ctx.profile).delete_old_versions(delete)


@awslambda.command(help="Generate concurrency report for AWS Lambda functions.")
@click.pass_obj
def concurrency(ctx: AppContext) -> None:
    LambdaService(ctx.profile).concurrency()
