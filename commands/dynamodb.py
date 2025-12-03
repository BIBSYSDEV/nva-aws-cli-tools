import click
from commands.services.dynamodb_service import DynamoDBService


@click.group()
def dynamodb():
    """Utility methods for working with DynamoDB tables."""
    pass


@dynamodb.command(help="Purge all items from a DynamoDB table.")
@click.argument("table")
@click.option(
    "--profile",
    envvar="AWS_PROFILE",
    default="default",
    help="The AWS profile to use. e.g. sikt-nva-sandbox, configure your profiles in ~/.aws/config",
)
def purge(table: str, profile: str) -> None:
    """
    Purge all items from a DynamoDB table.

    This command will:
    1. Find the table matching the provided name or partial name
    2. Show table information including approximate item count
    3. Request confirmation before deleting
    4. Delete all items from the table
    """
    service = DynamoDBService(profile)

    # Find the table
    result, _ = service.find_table(table)

    if result is None:
        click.echo(f"No table found matching '{table}'")
        return

    # Handle multiple matches
    if isinstance(result, list):
        click.echo(f"Multiple tables found matching '{table}':")
        for idx, table_name in enumerate(result, 1):
            click.echo(f"  {idx}. {table_name}")
        click.echo("\nPlease be more specific with the table name.")
        return

    table_name = result

    # Get table information
    try:
        table_info = service.get_table_info(table_name)
    except Exception as e:
        click.echo(f"Error retrieving table information: {str(e)}")
        return

    # Display table information
    click.echo("\nTable Information:")
    click.echo(f"  Profile: {profile}")
    click.echo(f"  Table Name: {table_info['table_name']}")
    click.echo(f"  Status: {table_info['table_status']}")
    click.echo(f"  Approximate Item Count: {table_info['item_count']}")

    # Get key schema
    partition_key, sort_key = service.get_key_names(table_name)
    key_schema_str = partition_key
    if sort_key:
        key_schema_str += f", {sort_key}"
    click.echo(f"  Key Schema: {key_schema_str}")

    # Confirmation
    click.echo(f"\nWARNING: This will delete ALL items from table '{table_name}'!")
    confirmation = click.prompt(
        "Type the full table name to confirm deletion",
        type=str,
    )

    if confirmation != table_name:
        click.echo("Deletion cancelled - table name did not match.")
        return

    # Perform deletion
    click.echo(f"\nStarting deletion of all items from '{table_name}'...")

    try:
        total_deleted = service.purge_table(table_name)
        click.echo(
            f"\n✓ Successfully deleted {total_deleted} items from '{table_name}'"
        )
    except Exception as e:
        click.echo(f"\n✗ Error during deletion: {str(e)}")
        raise
