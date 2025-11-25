#!/usr/bin/python3

import click
from commands.cognito import cognito
from commands.dlq import dlq
from commands.handle import handle
from commands.users import users
from commands.customers import customers
from commands.awslambda import awslambda
from commands.publications import publications
from commands.pipelines import pipelines
from commands.organization_migration import organization_migration
from commands.cristin import cristin
from commands.sqs import sqs
from commands.search import search

from log_config import configure_logger


@click.group()
def cli():
    configure_logger()
    pass


cli.add_command(cognito)
cli.add_command(dlq)
cli.add_command(handle)
cli.add_command(users)
cli.add_command(customers)
cli.add_command(awslambda)
cli.add_command(publications)
cli.add_command(pipelines)
cli.add_command(organization_migration)
cli.add_command(cristin)
cli.add_command(sqs)
cli.add_command(search)

if __name__ == "__main__":
    cli()
