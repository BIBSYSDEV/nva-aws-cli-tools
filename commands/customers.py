import click

from commands.services.customers_api import CustomersService
from commands.services.aws_utils import prettify

@click.group()
def customers():
    pass

@customers.command(help="Search customer references that does not exsist in the customer table")
@click.option('--profile', envvar='AWS_PROFILE', default='default', help='The AWS profile to use.')
def missing_customers(profile):
    result = CustomersService(profile).search_missing_customers()
    click.echo(prettify(result))

@customers.command(help="Search dubplicate customer references (same cristin id)")
@click.option('--profile', envvar='AWS_PROFILE', default='default', help='The AWS profile to use.')
def duplicate_customers(profile):
    result = CustomersService(profile).search_duplicate_customers()
    click.echo(prettify(result))