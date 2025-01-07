import click
from commands.services.lambda_api import LambdaService

@click.group()
def awslambda():
    pass

@awslambda.command(help="Delete old versions of AWS Lambda functions.")
@click.option('--profile', envvar='AWS_PROFILE', default='default', help='The AWS profile to use.')
@click.option('--delete', is_flag=True, default=True, help='Delete old versions.')
def delete_old_versions(profile, delete):
    LambdaService(profile).delete_old_versions(delete)