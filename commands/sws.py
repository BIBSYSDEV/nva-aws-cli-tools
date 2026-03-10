import click
import json
from functools import partial
from commands.services.sws import SwsService
from commands.services.sws_proxy import SwsProxyHandler, LocalTCPServer
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


@sws.command(name="proxy")
@click.option("--port", default=9200, show_default=True, help="Local port to listen on")
@click.option("--indices", default="", help="Comma-separated list of index names to expose")
@click.pass_obj
def start_proxy(app_context: AppContext, port: int, indices: str) -> None:
    """Start a local proxy to SWS API (localhost only, CORS enabled)"""
    service = SwsService(app_context.profile)
    index_list = [i.strip() for i in indices.split(",") if i.strip()]
    handler = partial(SwsProxyHandler, service, index_list)

    with LocalTCPServer(("127.0.0.1", port), handler) as server:
        click.echo(f"SWS proxy: http://127.0.0.1:{port}  →  {service.api_endpoint}")
        click.echo("Press Ctrl+C to stop")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            click.echo("\nProxy stopped.")
