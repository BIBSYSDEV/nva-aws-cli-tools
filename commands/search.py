import click
import json
import logging
from dataclasses import dataclass
from typing import Tuple

from commands.services.search_api import SearchApiService
from commands.utils import AppContext

logger = logging.getLogger(__name__)


@dataclass
class SearchParams:
    aggregation: str | None = None
    year_to: str | None = None
    year_from: str | None = None
    unit: str | None = None
    publisher: str | None = None
    contributor: str | None = None
    project: str | None = None
    funding_source: str | None = None
    funding_identifier: str | None = None
    category: str | None = None
    instance_type: str | None = None
    order: str | None = None
    sort: str = "relevance,identifier"

    PARAM_MAPPING = {
        "aggregation": "aggregation",
        "year_to": "publicationYearBefore",
        "year_from": "publicationYearSince",
        "unit": "unit",
        "publisher": "publisher",
        "contributor": "contributor",
        "project": "project",
        "funding_source": "fundingSource",
        "funding_identifier": "fundingIdentifier",
        "category": "category",
        "instance_type": "instanceType",
        "order": "order",
        "sort": "sort",
    }

    def to_query_params(self, api_domain: str) -> dict:
        query_params = {}

        for field_name, api_name in self.PARAM_MAPPING.items():
            value = getattr(self, field_name)
            if value:
                query_params[api_name] = value

        if "unit" in query_params and not query_params["unit"].startswith("http"):
            unit_id = query_params["unit"]
            query_params["unit"] = f"https://{api_domain}/cristin/organization/{unit_id}"

        if "project" in query_params and not query_params["project"].startswith("http"):
            project_id = query_params["project"]
            query_params["project"] = f"https://{api_domain}/cristin/project/{project_id}"

        return query_params

    @classmethod
    def from_kwargs(cls, **kwargs) -> "SearchParams":
        field_names = {f for f in cls.PARAM_MAPPING.keys()}
        filtered = {k: v for k, v in kwargs.items() if k in field_names}
        return cls(**filtered)


@click.group()
@click.pass_obj
def search(ctx: AppContext):
    """Search NVA resources."""
    pass


@search.command()
@click.pass_obj
@click.option(
    "--page-size",
    type=int,
    default=50,
    help="Number of results per page (default: 50)",
)
@click.option(
    "--limit",
    type=int,
    help="Maximum number of total results to return (default: unlimited)",
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
    "--funding-source",
    type=str,
    help="Funding source (e.g., NFR)",
)
@click.option(
    "--funding-identifier",
    type=str,
    help="Funding identifier (e.g., 357438)",
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
    ctx: AppContext,
    page_size: int,
    limit: int | None,
    id_only: bool,
    query: Tuple[str, ...],
    api_version: str,
    **kwargs,
) -> None:
    """Search NVA resources with flexible query parameters.

    This command provides automatic pagination and supports all search API parameters.

    Examples:
        # Search by unit and year range
        uv run cli.py search resources --unit 1965.0.0.0 --year-from 2025 --year-to 2026

        # Search by project (limit to first 100 results)
        uv run cli.py search resources --project 2744839 --limit 100

        # Search by funding source and identifier
        uv run cli.py search resources --funding-source NFR --funding-identifier 357438

        # Search by publisher (output only IDs)
        uv run cli.py search resources --publisher 08DC24C9-B7FF-4192-89AC-C629D93AD9CF --id-only

        # Use additional query parameters
        uv run cli.py search resources --query "funding=some-id" --query "status=published" --aggregation all
    """
    search_service = SearchApiService(profile=ctx.profile)
    search_params = SearchParams.from_kwargs(**kwargs)
    query_params = search_params.to_query_params(search_service.api_domain)

    for q in query:
        if "=" in q:
            key, value = q.split("=", 1)
            query_params[key] = value
        else:
            logger.warning(f"Ignoring invalid query parameter: {q}")

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
                print(json.dumps(hit, indent=2))
            count += 1

            if limit and count >= limit:
                break

        if count > 0:
            logger.info(f"Total results: {count}")

    except Exception as e:
        logger.error(f"Error fetching resources: {e}")
        raise click.Abort()
