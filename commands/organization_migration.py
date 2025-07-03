import click
import json
import uuid
from deepdiff import DeepDiff

from commands.services.search_api import SearchApiService
from commands.services.dynamodb_publications import DynamodbPublications
from commands.services.aws_utils import prettify
from commands.services.resource import Resource

table_pattern = (
    "^nva-resources-master-pipelines-NvaPublicationApiPipeline-.*-nva-publication-api$"
)


@click.group()
def organization_migration():
    pass


@organization_migration.command(help="List affected publications")
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.option(
    "--filename",
    default="report.json",
    help="the file name for report of usage of organization identifier",
)
@click.argument("organization_identifier", required=True, nargs=1)
def list_publications(
    profile: str, organization_identifier: str, filename: str
) -> dict:
    service = SearchApiService(profile)
    params = {"unit": organization_identifier}
    contributors_response = fetch_all(service, params)
    params = {"userAffiliation": organization_identifier}
    owner_response = fetch_all(service, params)
    report = prettify(format(contributors_response, owner_response))
    if filename:
        with open(filename, "w") as file:
            file.write(report)
    else:
        click.echo(report)


@organization_migration.command(help="Update publications")
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
@click.option(
    "--filename",
    default="report.json",
    help="file name of report in json",
)
@click.argument("old_organization_identifier", required=True, nargs=1)
@click.argument("new_organization_identifier", required=True, nargs=1)
def update_publications(
    profile: str,
    old_organization_identifier: str,
    new_organization_identifier: str,
    filename: str,
) -> dict:
    with open(filename, "r") as file:
        report = json.load(file)

    database = DynamodbPublications(profile, table_pattern)
    contributors = report.get("contributors", [])
    for identifier in contributors:
        (pk0, sk0, resource) = database.fetch_resource_by_identifier(identifier)
        bo = Resource(resource)
        bo.migrate_contributor_affiliations(
            old_organization_identifier, new_organization_identifier
        )

        diff = DeepDiff(resource, bo.get_data(), ignore_order=True)
        print(f"Updating {identifier}...")
        print(diff.pretty())
        database.update_resource(
            pk0, sk0, data=database.deflate_resource(bo.data), version=str(uuid.uuid4())
        )
    owners = report.get("owners", [])
    for identifier in owners:
        (pk0, sk0, resource) = database.fetch_resource_by_identifier(identifier)
        bo = Resource(resource)
        bo.migrate_owner_affiliation(
            old_organization_identifier, new_organization_identifier
        )

        diff = DeepDiff(resource, bo.data, ignore_order=True)
        click.echo(f"Updating {identifier}...")
        click.echo(diff.pretty())
        database.update_resource(
            pk0, sk0, data=database.deflate_resource(bo.data), version=str(uuid.uuid4())
        )


def fetch_all(service, params) -> list:
    default_params = {"sort": "modified_date:asc", "size": "100"}
    merged_params = {**default_params, **params}

    identifiers = set()
    modified_since = None

    while True:
        if modified_since:
            merged_params["modified_since"] = modified_since

        click.echo(f"Fetching next page of search hits: {merged_params}...")
        response = service.resource_search(merged_params)
        hits = response.get("hits", [])

        if not hits:
            click.echo("No more hits!")
            break

        for hit in hits:
            identifier = hit.get("identifier")
            if identifier:
                identifiers.add(identifier)

        last_modified = hits[-1].get("recordMetadata").get("modifiedDate")
        if not last_modified or last_modified == modified_since:
            click.echo("Page not changed!")
            break

        modified_since = last_modified

    return list(identifiers)


def format(contributors_response, owner_response) -> str:
    return {"contributors": contributors_response, "owners": owner_response}
