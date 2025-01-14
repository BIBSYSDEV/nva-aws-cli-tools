import boto3
import json
import os
import base64
import zlib
import re
from boto3.dynamodb.types import Binary


class DynamodbExport:
    def __init__(self, profile, table_pattern, condition, batch_size):
        self.condition = condition
        self.batch_size = batch_size
        self.table_pattern = table_pattern
        self.batch_counter = 0
        self.profile = profile

    def get_table(self):
        session = boto3.Session(profile_name=self.profile)
        dynamodb = session.client("dynamodb")
        response = dynamodb.list_tables()
        table_names = response["TableNames"]
        table_name = next(
            (name for name in table_names if re.match(self.table_pattern, name)), None
        )

        if table_name is None:
            print(f"No table found matching {self.table_pattern}")
            return

        dynamodb_resource = session.resource("dynamodb")
        return dynamodb_resource.Table(table_name)

    def _iterate_batches(self, table, custom_batch_processor):
        response = table.query(
            Limit=self.batch_size,
            KeyConditionExpression=self.condition,
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
            response = table.query(
                ExclusiveStartKey=response["LastEvaluatedKey"],
                Limit=self.batch_size,
                KeyConditionExpression=self.condition,
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

    def _inflate_batch(self, items):
        inflated_items = []
        for item in items:
            item = {
                k: (base64.b64encode(v.value).decode() if isinstance(v, Binary) else v)
                for k, v in item.items()
            }
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

    def save_to_folder(self, output_folder):
        self.output_folder = output_folder
        table = self.get_table()
        self._iterate_batches(table, self._save_inflated_items_to_file)

    def process(self, action):
        table = self.get_table()
        self._iterate_batches(table, action)


def get_account_alias(profile=None):
    # Create a default Boto3 session
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()

    # Create an IAM client
    iam = session.client("iam")

    # Get the account alias
    account_aliases = iam.list_account_aliases()["AccountAliases"]

    # Return the first account alias or None if the list is empty
    return account_aliases[0] if account_aliases else None
