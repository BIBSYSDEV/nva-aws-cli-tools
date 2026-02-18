import click
import json
from commands.services.sws import SwsService
from commands.utils import AppContext


@click.group()
def sws():
    """SWS (Search Web Service) operations"""
    pass


@sws.command(name="get-mappings")
@click.argument("index")
@click.pass_obj
def get_mappings(app_context: AppContext, index: str):
    """Get index mapping configuration

    INDEX is the name of the search index (e.g., 'resources', 'nvi-candidates')
    """
    service = SwsService(app_context.profile)
    mappings = service.get_mappings(index)

    if mappings:
        click.echo(json.dumps(mappings, indent=2))
    else:
        click.echo(f"Failed to retrieve mappings for index: {index}", err=True)
        raise SystemExit(1)
