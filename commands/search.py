import click
from rich.console import Console

from commands.services.search_api import SearchApiService

console = Console()


@click.group()
def search():
    """Search NVA resources."""
    pass


@search.command()
@click.argument("publisher_id", type=str)
@click.option("--profile", type=str, help="AWS profile to use")
@click.option(
    "--page-size",
    type=int,
    default=100,
    help="Number of results per page (default: 100)",
)
def publisher_publications(publisher_id, profile, page_size):
    """List all publication IDs for a given publisher.

    PUBLISHER_ID is the channel register ID (publisher UUID).

    Example:
        uv run cli.py search publisher-publications 08DC24C9-B7FF-4192-89AC-C629D93AD9CF
    """
    search_service = SearchApiService(profile=profile)

    query_params = {
        "aggregation": "all",
        "publisher": publisher_id,
        "order": "modifiedDate",
        "sort": "desc",
    }

    try:
        for hit in search_service.resource_search(query_params, page_size):
            identifier = hit.get("identifier")
            if identifier:
                print(identifier)
    except Exception as e:
        console.print(f"[red]Error fetching publications: {e}[/red]")
        raise click.Abort()
