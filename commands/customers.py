import click

from commands.services.customers_api import list_missing_customers, list_duplicate_customers
from commands.services.aws_utils import prettify

@click.group()
def customers():
    pass

@customers.command(help="Search customer references from users that does not exsist in the customer table")
@click.option('--profile', envvar='AWS_PROFILE', default='default', help='The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config')
def list_missing(profile) -> None:
    result = list_missing_customers(profile)
    click.echo(prettify(result))

@customers.command(help="Search dubplicate customer references (same cristin id)")
@click.option('--profile', envvar='AWS_PROFILE', default='default', help='The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config')
def list_duplicate(profile:str) -> None:
    result = list_duplicate_customers(profile)
    click.echo(prettify(result))