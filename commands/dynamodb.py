from datetime import datetime

import click
from boto3.dynamodb.conditions import Attr

from commands.utils import AppContext
from commands.services.dynamodb_exporter import GenericDynamodbExporter


@click.group()
@click.pass_obj
def dynamodb(ctx: AppContext):
    pass


@dynamodb.command(help="Export a DynamoDB table to JSONL files")
@click.option(
    "--table",
    required=True,
    help="Substring to match in the table name (e.g., 'resources', 'users', 'import-candidates')",
)
@click.option(
    "--folder",
    help="The folder to save the exported data (default: dynamodb_export_{table}_{timestamp})",
)
@click.option(
    "--filter",
    "filter_expression",
    help="Filter expression (e.g., 'PK0:begins_with:Resource:'). Format: 'attribute:operator:value'",
)
@click.pass_obj
def export(
    ctx: AppContext,
    table: str,
    folder: str | None,
    filter_expression: str | None,
) -> None:
    if folder is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        folder = f"dynamodb_export_{table}_{timestamp}"

    condition = None

    if filter_expression:
        condition = _parse_filter_expression(filter_expression)

    exporter = GenericDynamodbExporter(ctx.profile, table)
    exporter.export(folder, condition)


def _parse_filter_expression(filter_expression: str) -> Attr:
    parts = filter_expression.split(":")
    if len(parts) < 3:
        raise ValueError(
            "Filter expression must be in format 'attribute:operator:value' "
            "(e.g., 'PK0:begins_with:Resource:')"
        )

    attribute = parts[0]
    operator = parts[1]
    value = ":".join(parts[2:])

    attr = Attr(attribute)

    if operator == "begins_with":
        return attr.begins_with(value)
    elif operator == "eq":
        return attr.eq(value)
    elif operator == "ne":
        return attr.ne(value)
    elif operator == "contains":
        return attr.contains(value)
    elif operator == "exists":
        return attr.exists()
    elif operator == "not_exists":
        return attr.not_exists()
    elif operator == "gt":
        return attr.gt(value)
    elif operator == "gte":
        return attr.gte(value)
    elif operator == "lt":
        return attr.lt(value)
    elif operator == "lte":
        return attr.lte(value)
    else:
        raise ValueError(
            f"Unsupported operator '{operator}'. Supported operators: "
            "begins_with, eq, ne, contains, exists, not_exists, gt, gte, lt, lte"
        )
