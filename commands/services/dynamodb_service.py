import boto3
import re
import click


class DynamoDBService:
    def __init__(self, profile):
        self.profile = profile
        self.session = boto3.Session(profile_name=self.profile)
        self.dynamodb_client = self.session.client("dynamodb")
        self.dynamodb_resource = self.session.resource("dynamodb")

    def find_table(self, table_pattern):
        """
        Find a table matching the given pattern.

        Args:
            table_pattern (str): A pattern to match DynamoDB table name.

        Returns:
            tuple: (table_name, table_resource) or (None, None) if not found
        """
        response = self.dynamodb_client.list_tables()
        table_names = response["TableNames"]

        # Try exact match first
        if table_pattern in table_names:
            return table_pattern, self.dynamodb_resource.Table(table_pattern)

        # Try partial match
        matching_tables = [name for name in table_names if table_pattern.lower() in name.lower()]

        if not matching_tables:
            return None, None

        if len(matching_tables) == 1:
            table_name = matching_tables[0]
            return table_name, self.dynamodb_resource.Table(table_name)

        # Multiple matches - return them for user to choose
        return matching_tables, None

    def get_table_info(self, table_name):
        """
        Get table information including item count.

        Args:
            table_name (str): The DynamoDB table name.

        Returns:
            dict: Table information including ItemCount
        """
        table = self.dynamodb_resource.Table(table_name)
        table.reload()  # Refresh table metadata
        return {
            "table_name": table.name,
            "item_count": table.item_count,
            "table_status": table.table_status,
            "key_schema": table.key_schema,
        }

    def get_key_names(self, table_name):
        """
        Get the primary key and sort key names for a table.

        Args:
            table_name (str): The DynamoDB table name.

        Returns:
            tuple: (partition_key_name, sort_key_name) or (partition_key_name, None)
        """
        table = self.dynamodb_resource.Table(table_name)
        key_schema = table.key_schema

        partition_key = next((k["AttributeName"] for k in key_schema if k["KeyType"] == "HASH"), None)
        sort_key = next((k["AttributeName"] for k in key_schema if k["KeyType"] == "RANGE"), None)

        return partition_key, sort_key

    def purge_table(self, table_name):
        """
        Delete all items from a DynamoDB table.

        Args:
            table_name (str): The DynamoDB table name.

        Returns:
            int: Total number of items deleted
        """
        table = self.dynamodb_resource.Table(table_name)
        partition_key, sort_key = self.get_key_names(table_name)

        total_deleted = 0

        # Initial scan
        response = table.scan()
        total_deleted += self._delete_items(table, response["Items"], partition_key, sort_key)

        # Continue scanning if there are more items
        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            total_deleted += self._delete_items(table, response["Items"], partition_key, sort_key)
            click.echo(f"Deletion in progress... {total_deleted} items deleted")

        return total_deleted

    def _delete_items(self, table, items, partition_key, sort_key):
        """
        Delete a batch of items from the table.

        Args:
            table: DynamoDB table resource
            items: List of items to delete
            partition_key: Name of the partition key
            sort_key: Name of the sort key (can be None)

        Returns:
            int: Number of items deleted
        """
        if not items:
            return 0

        with table.batch_writer() as batch:
            for item in items:
                key = {partition_key: item[partition_key]}
                if sort_key:
                    key[sort_key] = item[sort_key]
                batch.delete_item(Key=key)

        return len(items)
