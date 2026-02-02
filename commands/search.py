import click
import logging

from commands.services.search_api import SearchApiService

logger = logging.getLogger(__name__)


@click.group()
def search():
    """Search NVA resources."""
    pass


@search.command()
@click.option("--profile", type=str, help="AWS profile to use")
@click.option(
    "--page-size",
    type=int,
    default=50,
    help="Number of results per page (default: 100)",
)
@click.option(
    "--aggregation",
    type=str,
    default="none",
    help="Aggregation parameter (default: none)",
)
@click.option(
    "--year-to",
    type=str,
    help="Publication year before (e.g., 2026)",
)
@click.option(
    "--year-from",
    type=str,
    help="Publication year since (e.g., 2025)",
)
@click.option(
    "--unit",
    type=str,
    help="Unit/organization ID (e.g., 1965.0.0.0)",
)
@click.option(
    "--publisher",
    type=str,
    help="Publisher UUID",
)
@click.option(
    "--contributor",
    type=str,
    help="Contributor ID",
)
@click.option(
    "--project",
    type=str,
    help="Project ID (e.g., 2744839 or full URL)",
)
@click.option(
    "--category",
    type=str,
    help="Category filter",
)
@click.option(
    "--instance-type",
    type=str,
    help="Instance type filter",
)
@click.option(
    "--order",
    type=str,
    help="Order field (e.g., modifiedDate, createdDate)",
)
@click.option(
    "--sort",
    type=str,
    default="relevance,identifier",
    help="Sort order (default: relevance,identifier)",
)
@click.option(
    "--id-only",
    is_flag=True,
    help="Output only identifiers (one per line)",
)
@click.option(
    "--query",
    type=str,
    multiple=True,
    help="Additional query parameters in format key=value (can be used multiple times)",
)
@click.option(
    "--api-version",
    type=str,
    default="2024-12-01",
    help="API version to use (default: 2024-12-01)",
)
def resources(
    profile,
    page_size,
    aggregation,
    year_to,
    year_from,
    unit,
    publisher,
    contributor,
    project,
    category,
    instance_type,
    order,
    sort,
    id_only,
    query,
    api_version,
):
    """Search NVA resources with flexible query parameters.

    This command provides automatic pagination and supports all search API parameters.

    Examples:
        # Search by unit and year range
        uv run cli.py search resources --unit 1965.0.0.0 --year-from 2025 --year-to 2026

        # Search by project
        uv run cli.py search resources --project 2744839

        # Search by publisher (output only IDs)
        uv run cli.py search resources --publisher 08DC24C9-B7FF-4192-89AC-C629D93AD9CF --id-only

        # Use additional query parameters
        uv run cli.py search resources --query "funding=some-id" --query "status=published" --aggregation all
    """
    search_service = SearchApiService(profile=profile)

    query_params = {}

    if aggregation:
        query_params["aggregation"] = aggregation
    if year_to:
        query_params["publicationYearBefore"] = year_to
    if year_from:
        query_params["publicationYearSince"] = year_from
    if unit:
        if not unit.startswith("http"):
            api_domain = search_service.api_domain
            unit = f"https://{api_domain}/cristin/organization/{unit}"
        query_params["unit"] = unit
    if publisher:
        query_params["publisher"] = publisher
    if contributor:
        query_params["contributor"] = contributor
    if project:
        if not project.startswith("http"):
            api_domain = search_service.api_domain
            project = f"https://{api_domain}/cristin/project/{project}"
        query_params["project"] = project
    if category:
        query_params["category"] = category
    if instance_type:
        query_params["instanceType"] = instance_type
    if order:
        query_params["order"] = order

    query_params["sort"] = sort

    for q in query:
        if "=" in q:
            key, value = q.split("=", 1)
            query_params[key] = value
        else:
            logger.warning(f"Ignoring invalid query parameter: {q}")

    if not query_params:
        logger.warning("No query parameters specified. This may return many results.")

    try:
        count = 0
        for hit in search_service.resource_search(
            query_params, page_size, api_version=api_version
        ):
            if id_only:
                identifier = hit.get("identifier")
                if identifier:
                    print(identifier)
            else:
                import json

                print(json.dumps(hit, indent=2))
            count += 1

        if count > 0:
            logger.info(f"Total results: {count}")

    except Exception as e:
        logger.error(f"Error fetching resources: {e}")
        raise click.Abort()
