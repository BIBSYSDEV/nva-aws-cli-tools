import json
from typing import Dict, List, Tuple, Optional, Callable
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import boto3


class BatchJobType(Enum):
    """Supported batch job types for resource operations."""

    REINDEX_RECORD = "REINDEX_RECORD"


class ResourceBatchJobService:
    """Service for handling resource batch job operations via SQS."""

    def __init__(self, profile: str):
        self.profile = profile
        self.session = boto3.Session(profile_name=profile)
        self.sqs = self.session.client("sqs")
        self._queue_url = None
        self._find_batch_job_queue()

    def _find_batch_job_queue(self) -> None:
        try:
            response = self.sqs.list_queues()
            all_queues = response.get("QueueUrls", [])

            # Filter queues that match the pattern
            matching_queues = [
                q for q in all_queues if "DynamodbResourceBatchJobWorkQueue" in q
            ]

            if matching_queues:
                # Cache the first matching queue
                self._queue_url = matching_queues[0]

        except Exception as e:
            print(f"Error finding queue: {str(e)}")

    def _create_batch_job_message(
        self,
        resource_id: str,
        job_type: BatchJobType,
        parameters: Optional[Dict] = None,
    ) -> Dict:
        if parameters is None:
            parameters = {}

        return {
            "dynamoDbKey": {
                "partitionKey": f"Resource:{resource_id}",
                "sortKey": f"Resource:{resource_id}",
                "indexName": "ResourcesByIdentifier",
            },
            "jobType": job_type.value,
            "parameters": parameters,
        }

    def _create_reindex_message(self, publication_id: str) -> Dict:
        return self._create_batch_job_message(
            resource_id=publication_id, job_type=BatchJobType.REINDEX_RECORD
        )

    def _send_batch(
        self, messages: List[Dict], queue_url: str
    ) -> Tuple[int, int, List[Dict]]:
        try:
            # Format messages for SQS batch send
            sqs_messages = [
                {"Id": str(i), "MessageBody": json.dumps(msg)}
                for i, msg in enumerate(messages)
            ]

            response = self.sqs.send_message_batch(
                QueueUrl=queue_url, Entries=sqs_messages
            )

            successful = len(response.get("Successful", []))
            failed = response.get("Failed", [])

            return successful, len(failed), failed

        except Exception as e:
            print(f"Error sending batch: {str(e)}")
            return 0, len(messages), [{"Message": str(e)}]

    def process_batch_job(
        self,
        input_file: str,
        job_type: BatchJobType,
        batch_size: int = 10,
        parameters: Optional[Dict] = None,
        progress_callback: Optional[Callable[[int, int, int, int], None]] = None,
        concurrency: int = 3,
    ) -> Dict:
        # Check if queue was found during initialization
        if not self._queue_url:
            return {
                "success": False,
                "error": "No DynamodbResourceBatchJobWorkQueue found",
                "total_processed": 0,
                "successful": 0,
                "failed": 0,
            }

        # Read resource IDs from file
        with open(input_file, "r") as f:
            resource_ids = [line.strip() for line in f if line.strip()]

        total_ids = len(resource_ids)

        # Create batches
        batches = []
        batch_messages = []

        for resource_id in resource_ids:
            message = self._create_batch_job_message(
                resource_id=resource_id, job_type=job_type, parameters=parameters
            )
            batch_messages.append(message)

            if len(batch_messages) >= batch_size:
                batches.append(batch_messages)
                batch_messages = []

        # Add remaining messages as final batch
        if batch_messages:
            batches.append(batch_messages)

        # Thread-safe counters
        total_sent = 0
        failed_count = 0
        all_failures = []
        lock = Lock()

        def send_batch_wrapper(batch_data):
            """Wrapper to send a batch and handle results."""
            _, messages = batch_data  # batch_num not needed currently
            successful, failed, failures = self._send_batch(messages, self._queue_url)

            with lock:
                nonlocal total_sent, failed_count
                total_sent += successful
                failed_count += failed
                all_failures.extend(failures)

                # Report progress if callback provided
                if progress_callback:
                    progress_callback(successful, len(messages), total_sent, total_ids)

                # Report any failures for this batch
                if failures:
                    for failure in failures:
                        print(
                            f"Failed to send message {failure.get('Id', 'unknown')}: {failure.get('Message', 'Unknown error')}"
                        )

            return successful, failed

        # Process batches concurrently
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            # Submit all batches with their index
            futures = [
                executor.submit(send_batch_wrapper, (i, batch))
                for i, batch in enumerate(batches)
            ]

            # Wait for all batches to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing batch: {str(e)}")

        return {
            "success": total_sent == total_ids,
            "job_type": job_type.value,
            "total_processed": total_ids,
            "successful": total_sent,
            "failed": failed_count,
            "failures": all_failures,
        }

    def process_reindex_job(
        self,
        input_file: str,
        batch_size: int = 10,
        progress_callback: Optional[Callable[[int, int, int, int], None]] = None,
        concurrency: int = 3,
    ) -> Dict:
        return self.process_batch_job(
            input_file=input_file,
            job_type=BatchJobType.REINDEX_RECORD,
            batch_size=batch_size,
            progress_callback=progress_callback,
            concurrency=concurrency,
        )
