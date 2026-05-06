import json

import click

from commands.services.sws import SwsClient, get_mappings
from commands.utils import AppContext


@click.group()
def sws():
    """SWS (Search Web Service) operations"""
    pass


@sws.command(name="get-mappings")
@click.argument("index")
@click.pass_obj
def get_mappings_command(ctx: AppContext, index: str):
    """Get index mapping configuration

    INDEX is the name of the search index (e.g., 'resources', 'nvi-candidates')
    """
    client = SwsClient(session=ctx.session, profile=ctx.profile)
    mappings = get_mappings(client, index)

    if mappings:
        click.echo(json.dumps(mappings, indent=2))
    else:
        click.echo(f"Failed to retrieve mappings for index: {index}", err=True)
        raise SystemExit(1)
