from __future__ import annotations

import base64
import json
import logging
import math
import threading
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        self.profile = profile
        self.table_name_substring = table_name_substring
        self.session = (
            boto3.Session(profile_name=profile) if profile else boto3.Session()
        )
        self.dynamodb = self.session.client("dynamodb")
        self.table = self._get_table()
        self.table_name: str = self.table.name
        self._thread_local = threading.local()

    def _get_table(self) -> Any:
        response = self.dynamodb.list_tables()
        table_names = response["TableNames"]
        table_name = next(
            (name for name in table_names if self.table_name_substring in name), None
        )

        if table_name is None:
            raise ValueError(f"No table found containing '{self.table_name_substring}'")

        dynamodb_resource = self.session.resource("dynamodb")
        return dynamodb_resource.Table(table_name)

    def _decompress_data(self, data_field: Any) -> dict[str, Any] | None:
        if not isinstance(data_field, (bytes, str, Binary)):
            return None
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
        if "data" not in item:
            return item

        decompressed_data = self._decompress_data(item["data"])
        if decompressed_data:
            return {**item, "@data_decompressed": decompressed_data}

        return item

    def _get_table_for_thread(self) -> Any:
        if not hasattr(self._thread_local, "table"):
            session = (
                boto3.Session(profile_name=self.profile)
                if self.profile
                else boto3.Session()
            )
            self._thread_local.table = session.resource("dynamodb").Table(
                self.table_name
            )
        return self._thread_local.table

    def _save_items_to_file(
        self,
        items: list[dict[str, Any]],
        batch_count: int,
        output_folder: str,
        segment: int | None = None,
    ) -> None:
        processed_items = [self._process_item(item) for item in items]

        if segment is not None:
            output_file = (
                Path(output_folder)
                / f"segment_{segment:03d}_batch_{batch_count:05d}.jsonl"
            )
        else:
            output_file = Path(output_folder) / f"batch_{batch_count:05d}.jsonl"

        with open(output_file, "w", encoding="utf-8") as file:
            for item in processed_items:
                json.dump(item, file, cls=DynamoDBEncoder)
                file.write("\n")

        logger.info(f"Saved {len(processed_items)} items to {output_file}")

    def _run_scan_loop(
        self,
        table: Any,
        scan_kwargs: dict[str, Any],
        limit: int | None,
        callback: Any,
        progress_bar: tqdm,
        progress_lock: threading.Lock | None = None,
    ) -> tuple[int, int]:
        batch_count = 0
        items_processed = 0
        items_scanned = 0

        response = table.scan(**scan_kwargs)
        items = response.get("Items", [])

        while True:
            items_scanned += response.get("ScannedCount", 0)

            if items:
                if limit is not None:
                    items = items[: limit - items_processed]

                batch_count += 1
                items_processed += len(items)

                if progress_lock:
                    with progress_lock:
                        progress_bar.update(len(items))
                else:
                    progress_bar.update(len(items))

                callback(items, batch_count)

            if "LastEvaluatedKey" not in response:
                break

            if limit is not None and items_processed >= limit:
                break

            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
            response = table.scan(**scan_kwargs)
            items = response.get("Items", [])

        return items_processed, items_scanned

    def _iterate_batches_scan(
        self,
        condition: ConditionBase | None,
        callback: Any,
        limit: int | None = None,
    ) -> None:
        scan_kwargs: dict[str, Any] = {}
        if condition:
            scan_kwargs["FilterExpression"] = condition

        logger.info("Initiating table scan (this may take a while for large tables)...")

        with tqdm(desc="Scanning table", unit="items") as progress_bar:
            items_processed, _ = self._run_scan_loop(
                self.table, scan_kwargs, limit, callback, progress_bar
            )

        logger.info(f"Completed scan. Processed {items_processed} items")

    def _scan_segment(
        self,
        segment: int,
        total_segments: int,
        condition: ConditionBase | None,
        callback: Any,
        limit: int | None,
        progress_bar: tqdm,
        progress_lock: threading.Lock,
    ) -> tuple[int, int]:
        table = self._get_table_for_thread()
        scan_kwargs: dict[str, Any] = {
            "Segment": segment,
            "TotalSegments": total_segments,
        }
        if condition:
            scan_kwargs["FilterExpression"] = condition

        def segment_callback(items: list, batch_count: int) -> None:
            callback(items, segment, batch_count)

        return self._run_scan_loop(
            table, scan_kwargs, limit, segment_callback, progress_bar, progress_lock
        )

    def _iterate_batches_parallel_scan(
        self,
        condition: ConditionBase | None,
        callback: Any,
        limit: int | None,
        total_segments: int,
    ) -> None:
        logger.info(
            f"Initiating parallel table scan with {total_segments} segments "
            "(this may take a while for large tables)..."
        )

        progress_lock = threading.Lock()
        total_items_processed = 0
        total_items_scanned = 0
        per_segment_limit = (
            math.ceil(limit / total_segments) if limit is not None else None
        )

        with tqdm(desc="Scanning table (parallel)", unit="items") as progress_bar:
            with ThreadPoolExecutor(max_workers=total_segments) as executor:
                futures = {
                    executor.submit(
                        self._scan_segment,
                        segment,
                        total_segments,
                        condition,
                        callback,
                        per_segment_limit,
                        progress_bar,
                        progress_lock,
                    ): segment
                    for segment in range(total_segments)
                }

                for future in as_completed(futures):
                    items_processed, items_scanned = future.result()
                    total_items_processed += items_processed
                    total_items_scanned += items_scanned

        logger.info(
            f"Completed parallel scan. Processed {total_items_processed} items "
            f"across {total_segments} segments"
        )

    def export(
        self,
        output_folder: str,
        condition: ConditionBase | None = None,
        limit: int | None = None,
        total_segments: int = 1,
    ) -> None:
        Path(output_folder).mkdir(parents=True, exist_ok=True)

        logger.info(f"Starting export of table {self.table_name} to {output_folder}")

        if total_segments > 1:

            def parallel_save_callback(
                items: list, segment: int, batch_count: int
            ) -> None:
                self._save_items_to_file(
                    items, batch_count, output_folder, segment=segment
                )

            self._iterate_batches_parallel_scan(
                condition, parallel_save_callback, limit, total_segments
            )
        else:

            def save_callback(items: list, batch_count: int) -> None:
                self._save_items_to_file(items, batch_count, output_folder)

            self._iterate_batches_scan(condition, save_callback, limit)
