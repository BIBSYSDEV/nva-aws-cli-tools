import boto3
import json
import os
import base64
import zlib
import re
from boto3.dynamodb.types import Binary
from boto3.dynamodb.conditions import Key


class DynamodbPublications:
    def __init__(self, profile, table_pattern):
        """
        Initializes the DynamoDB service with the specified parameters.

        Args:
            profile (str): The AWS profile to use for authentication.
            table_pattern (str): A pattern to match DynamoDB table name.
            condition (Attr): The condition to filter items during export.
        """
        self.table_pattern = table_pattern
        self.batch_counter = 0
        self.profile = profile
        self.session = boto3.Session(profile_name=self.profile)
        self.dynamodb = self.session.client("dynamodb")
        self.table = self.get_table()

    def get_table(self):
        response = self.dynamodb.list_tables()
        table_names = response["TableNames"]
        table_name = next(
            (name for name in table_names if re.match(self.table_pattern, name)), None
        )

        if table_name is None:
            print(f"No table found matching {self.table_pattern}")
            return

        dynamodb_resource = self.session.resource("dynamodb")
        return dynamodb_resource.Table(table_name)

    def _iterate_batches_scan(self, condition, batch_size, custom_batch_processor):
        scan_params = {
            "Limit": batch_size,
            "ReturnConsumedCapacity": "TOTAL",
        }
        if condition:
            scan_params["FilterExpression"] = condition

        response = self.table.scan(**scan_params)
        items = response["Items"]
        batch = self._inflate_batch(items)
        custom_batch_processor(batch, self.batch_counter)
        self.batch_counter += 1
        total_count = len(items)
        total_consumed_capacity = response["ConsumedCapacity"]["CapacityUnits"]
        print(
            f"Processed {len(items)} items, Total: {total_count}, ConsumedCapacity: {total_consumed_capacity}"
        )

        while "LastEvaluatedKey" in response:
            scan_params["ExclusiveStartKey"] = response["LastEvaluatedKey"]
            response = self.table.scan(**scan_params)
            items = response["Items"]
            if items:
                batch = self._inflate_batch(items)
                custom_batch_processor(batch, self.batch_counter)
                self.batch_counter += 1
                total_count += len(items)
                total_consumed_capacity += response["ConsumedCapacity"]["CapacityUnits"]
                print(
                    f"Processed {len(items)} items, Total: {total_count}, ConsumedCapacity: {total_consumed_capacity}"
                )

    def _iterate_batches_query(self, condition, batch_size, custom_batch_processor):
        response = self.table.query(
            Limit=batch_size,
            KeyConditionExpression=condition,
            ReturnConsumedCapacity="TOTAL",
        )
        items = response["Items"]
        batch = self._inflate_batch(items)
        custom_batch_processor(batch, self.batch_counter)
        self.batch_counter += 1
        total_count = len(items)
        total_consumed_capacity = response["ConsumedCapacity"]["CapacityUnits"]
        print(
            f"Processed {len(items)} items, Total: {total_count}, ConsumedCapacity: {total_consumed_capacity}"
        )

        while "LastEvaluatedKey" in response:
            response = self.table.query(
                ExclusiveStartKey=response["LastEvaluatedKey"],
                Limit=batch_size,
                KeyConditionExpression=condition,
                ReturnConsumedCapacity="TOTAL",
            )
            items = response["Items"]
            if items:
                batch = self._inflate_batch(items)
                custom_batch_processor(batch, self.batch_counter)
                self.batch_counter += 1
                total_count += len(items)
                total_consumed_capacity += response["ConsumedCapacity"]["CapacityUnits"]
                print(
                    f"Processed {len(items)} items, Total: {total_count}, ConsumedCapacity: {total_consumed_capacity}"
                )

    def _encode_binary_attributes(self, item):
        return {
            k: (base64.b64encode(v.value).decode() if isinstance(v, Binary) else v)
            for k, v in item.items()
        }

    def _inflate_batch(self, items):
        inflated_items = []
        for item in items:
            item = self._encode_binary_attributes(item)
            inflated_item = self._inflate_item(item)
            inflated_items.append(inflated_item)
        return inflated_items

    def _inflate_item(self, item):
        if "data" in item:
            data = item["data"]
            decoded_data = base64.b64decode(data)
            inflated_data = zlib.decompress(decoded_data, -zlib.MAX_WBITS)
            inflated_str = inflated_data.decode("utf-8")
            return json.loads(inflated_str)

    def _save_inflated_items_to_file(self, inflated_items, batch_counter):
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        filename = os.path.join(self.output_folder, f"batch_{batch_counter}.jsonl")
        with open(filename, "w") as file:
            for inflated_item in inflated_items:
                file.write(json.dumps(inflated_item))
                file.write("\n")

    def save_to_folder(self, condition, batch_size, output_folder):
        self.output_folder = output_folder
        self._iterate_batches_scan(
            condition, batch_size, self._save_inflated_items_to_file
        )

    def process_query(self, condition, batch_size, batch_action):
        self._iterate_batches_query(condition, batch_size, batch_action)

    def process_scan(self, condition, batch_size, batch_action):
        self._iterate_batches_scan(condition, batch_size, batch_action)

    def fetch_resource_by_identifier(self, identifier):
        response = self.table.query(
            IndexName="ResourcesByIdentifier",
            KeyConditionExpression=Key("PK3").eq(f"Resource:{identifier}")
            & Key("SK3").eq(f"Resource:{identifier}"),
            Limit=1,
        )
        items = response.get("Items", [])
        if not items:
            return None, None, None

        item = items[0]
        inflated_item = self._inflate_item(self._encode_binary_attributes(item))
        return item.get("PK0"), item.get("SK0"), inflated_item

    def deflate_resource(self, resource):
        resource_str = json.dumps(resource)
        compress_obj = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        compressed_data = (
            compress_obj.compress(resource_str.encode()) + compress_obj.flush()
        )
        return Binary(compressed_data)

    def update_resource(self, pk0, sk0, **kwargs):
        """
        Update attributes in the DynamoDB table for the given primary and sort keys.

        :param pk0: Primary key value
        :param sk0: Sort key value
        :param kwargs: Attributes to update in the item (key-value pairs)

        Example usage:
        service.update_resource(
            pk0="Resource:12345@123.0.0.0",  # Example primary key
            sk0="Resource:12345",  # Example sort key
            data={"some": "data"},  # Example data attribute
            PK4="CristinIdentifier:12345"  # Additional attribute
        )
        """
        update_statement = self.prepare_update_resource(pk0, sk0, **kwargs)
        self.execute_batch_updates([update_statement])

    def prepare_update_resource(self, pk0, sk0, **kwargs):
        """
        Prepare a transaction for updating an item.
        Returns a dictionary representing the update transaction.

        :param pk0: Primary key value
        :param sk0: Sort key value
        :param kwargs: Attributes to update in the item (key-value pairs)

        Example usage:
        service.update_resource(
            pk0="Resource:12345@123.0.0.0",  # Example primary key
            sk0="Resource:12345",  # Example sort key
            data={"some": "data"},  # Example data attribute
            PK4="CristinIdentifier:12345"  # Additional attribute
        )
        """
        update_expression = []
        expression_attribute_names = {}
        expression_attribute_values = {}

        for idx, (key, value) in enumerate(kwargs.items(), start=1):
            placeholder_name = f"#attr{idx}"
            placeholder_value = f":val{idx}"

            update_expression.append(f"{placeholder_name} = {placeholder_value}")
            expression_attribute_names[placeholder_name] = key
            expression_attribute_values[placeholder_value] = value

        update_expression_str = "SET " + ", ".join(update_expression)

        return {
            "Update": {
                "TableName": self.table.name,  # Use the resolved table name
                "Key": {"PK0": {"S": pk0}, "SK0": {"S": sk0}},
                "UpdateExpression": update_expression_str,
                "ExpressionAttributeNames": expression_attribute_names,
                "ExpressionAttributeValues": self._create_expression_attribute_values(
                    expression_attribute_values
                ),
            }
        }

    def _create_expression_attribute_values(self, expression_attribute_values):
        def convert_value(v):
            if isinstance(v, str):
                return {"S": v}
            elif isinstance(v, (int, float)):
                return {"N": str(v)}
            elif isinstance(v, bytes):
                return {"B": v}
            elif isinstance(v, Binary):  # For `Binary` type from Boto3
                return {"B": bytes(v)}
            elif isinstance(v, bool):
                return {"BOOL": v}
            elif v is None:
                return {"NULL": True}
            elif isinstance(v, set):
                if all(isinstance(i, str) for i in v):
                    return {"SS": v}
                elif all(isinstance(i, (int, float)) for i in v):
                    return {"NS": {str(i) for i in v}}
                elif all(isinstance(i, (bytes, Binary)) for i in v):
                    return {"BS": [bytes(i) for i in v]}
            elif isinstance(v, dict):
                return {"M": v}
            elif isinstance(v, list):
                return {
                    "L": [
                        convert_value(i)
                        if isinstance(
                            i, (str, int, float, bytes, Binary, bool, type(None))
                        )
                        else ValueError(f"Unsupported list item type: {type(i)}")
                        for i in v
                    ]
                }
            else:
                raise ValueError(f"Unsupported value type: {type(v)}")

        return {k: convert_value(v) for k, v in expression_attribute_values.items()}

    def execute_batch_updates(self, transact_items):
        self.dynamodb.transact_write_items(TransactItems=transact_items)
