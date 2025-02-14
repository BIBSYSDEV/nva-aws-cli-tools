#!/usr/bin/python3

import click
from commands.cognito import cognito
from commands.handle import handle
from commands.users import users
from commands.customers import customers
from commands.awslambda import awslambda
from commands.publications import publications

@click.group()
def cli():
    pass

cli.add_command(cognito)
cli.add_command(handle)
cli.add_command(users)
cli.add_command(customers)
cli.add_command(awslambda)
cli.add_command(publications)

if __name__ == "__main__":
    cli()