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
    help="The folder to save the exported data (default: dynamodb_export_{profile}_{table}_{timestamp})",
)
@click.option(
    "--filter",
    "filter_expressions",
    multiple=True,
    help="Filter expression (e.g., 'PK0:begins_with:Resource:'). Format: 'attribute:operator:value'. Can be specified multiple times (combined with AND logic)",
)
@click.pass_obj
def export(
    ctx: AppContext,
    table: str,
    folder: str | None,
    filter_expressions: tuple[str, ...],
) -> None:
    if folder is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        folder = f"dynamodb_export_{ctx.profile}_{table}_{timestamp}"

    condition = None

    if filter_expressions:
        condition = _parse_multiple_filters(filter_expressions)

    exporter = GenericDynamodbExporter(ctx.profile, table)
    exporter.export(folder, condition)


def _parse_multiple_filters(filter_expressions: tuple[str, ...]) -> Attr:
    """Parse multiple filter expressions and combine them with AND logic."""
    if not filter_expressions:
        return None

    conditions = [_parse_filter_expression(expr) for expr in filter_expressions]

    # Combine all conditions with AND logic
    combined_condition = conditions[0]
    for condition in conditions[1:]:
        combined_condition = combined_condition & condition

    return combined_condition


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
