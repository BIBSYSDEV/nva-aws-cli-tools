import base64
import json
import logging
import zlib
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
from boto3.dynamodb.conditions import ConditionBase
from boto3.dynamodb.types import Binary
from tqdm import tqdm

logger = logging.getLogger(__name__)


class DynamoDBEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj) if obj % 1 else int(obj)
        if isinstance(obj, Binary):
            return base64.b64encode(bytes(obj)).decode("utf-8")
        if isinstance(obj, bytes):
            return base64.b64encode(obj).decode("utf-8")
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        return super().default(obj)


class GenericDynamodbExporter:
    def __init__(self, profile: str | None, table_name_substring: str) -> None:
        self.table_name_substring = table_name_substring
        self.session = boto3.Session(profile_name=profile) if profile else boto3.Session()
        self.dynamodb = self.session.client("dynamodb")
        self.table = self._get_table()
        self.table_name: str | None = self.table.name if self.table else None
        self.output_folder: str | None = None

    def _get_table(self) -> Any | None:
        response = self.dynamodb.list_tables()
        table_names = response["TableNames"]
        table_name = next(
            (name for name in table_names if self.table_name_substring in name), None
        )

        if table_name is None:
            logger.error(f"No table found containing '{self.table_name_substring}'")
            return None

        dynamodb_resource = self.session.resource("dynamodb")
        return dynamodb_resource.Table(table_name)

    def _detect_compression(self, item: dict[str, Any]) -> bool:
        if "data" not in item:
            return False

        try:
            data = item["data"]
            if isinstance(data, Binary):
                decoded_data = bytes(data)
            elif isinstance(data, bytes):
                decoded_data = data
            else:
                decoded_data = base64.b64decode(data)

            zlib.decompress(decoded_data, -zlib.MAX_WBITS)
            return True
        except Exception:
            return False

    def _decompress_data(self, data_field: bytes | str | Binary) -> dict[str, Any] | None:
        try:
            if isinstance(data_field, Binary):
                decoded_data = bytes(data_field)
            elif isinstance(data_field, bytes):
                decoded_data = data_field
            else:
                decoded_data = base64.b64decode(data_field)

            inflated_data = zlib.decompress(decoded_data, -zlib.MAX_WBITS)
            inflated_str = inflated_data.decode("utf-8")
            return json.loads(inflated_str)
        except Exception as error:
            logger.error(f"Failed to decompress data: {error}")
            return None

    def _process_item(self, item: dict[str, Any]) -> dict[str, Any]:
        if "data" in item and self._detect_compression(item):
            decompressed_data = self._decompress_data(item["data"])
            if decompressed_data:
                return {**item, "@data_decompressed": decompressed_data}

        return item

    def _save_items_to_file(self, items: list[dict[str, Any]], batch_count: int) -> None:
        processed_items = [self._process_item(item) for item in items]

        output_file = Path(self.output_folder) / f"batch_{batch_count:05d}.jsonl"
        with open(output_file, "w", encoding="utf-8") as file:
            for item in processed_items:
                json.dump(item, file, cls=DynamoDBEncoder)
                file.write("\n")

        logger.info(f"Saved {len(processed_items)} items to {output_file}")

    def _iterate_batches_scan(
        self,
        condition: ConditionBase | None,
        callback: Any,
    ) -> None:
        scan_kwargs = {}
        if condition:
            scan_kwargs["FilterExpression"] = condition

        batch_count = 0
        items_processed = 0

        logger.info("Initiating table scan (this may take a while for large tables)...")
        response = self.table.scan(**scan_kwargs)
        items = response.get("Items", [])
        logger.info(f"Received first batch with {len(items)} items")

        with tqdm(desc="Scanning table", unit="items") as progress_bar:
            while True:
                if items:
                    batch_count += 1
                    items_processed += len(items)
                    progress_bar.update(len(items))
                    callback(items, batch_count)

                if "LastEvaluatedKey" not in response:
                    break

                scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
                response = self.table.scan(**scan_kwargs)
                items = response.get("Items", [])

        logger.info(f"Completed scan. Processed {items_processed} items in {batch_count} batches")

    def export(
        self,
        output_folder: str,
        condition: ConditionBase | None = None,
    ) -> None:
        if not self.table:
            logger.error("Table not found. Cannot export.")
            return

        self.output_folder = output_folder
        Path(output_folder).mkdir(parents=True, exist_ok=True)

        logger.info(f"Starting export of table {self.table_name} to {output_folder}")

        self._iterate_batches_scan(condition, self._save_items_to_file)
