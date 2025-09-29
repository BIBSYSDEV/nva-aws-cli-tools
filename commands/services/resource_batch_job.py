import json
import time
from typing import Dict, List, Tuple, Optional
from enum import Enum

import boto3


class BatchJobType(Enum):
    """Supported batch job types for resource operations."""
    REINDEX_RECORD = "REINDEX_RECORD"


class ResourceBatchJobService:
    """Service for handling resource batch job operations via SQS."""
    
    def __init__(self, profile: str):
        """
        Initialize the ResourceBatchJobService with AWS profile.
        
        Args:
            profile: AWS profile name for session
        """
        self.profile = profile
        self.session = boto3.Session(profile_name=profile)
        self.sqs = self.session.client("sqs")
        self._queue_url = None
    
    def find_batch_job_queue(self) -> Optional[str]:
        """
        Find the DynamodbResourceBatchJobWorkQueue in the current AWS account.
        
        Returns:
            Queue URL if found, None otherwise
        """
        if self._queue_url:
            return self._queue_url
            
        try:
            response = self.sqs.list_queues()
            all_queues = response.get("QueueUrls", [])
            
            # Filter queues that match the pattern
            matching_queues = [
                q for q in all_queues if "DynamodbResourceBatchJobWorkQueue" in q
            ]
            
            if not matching_queues:
                return None
            
            # Cache and return the first matching queue
            self._queue_url = matching_queues[0]
            return self._queue_url
            
        except Exception as e:
            print(f"Error finding queue: {str(e)}")
            return None
    
    def create_batch_job_message(
        self,
        resource_id: str,
        job_type: BatchJobType,
        parameters: Optional[Dict] = None
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
    
    def create_reindex_message(self, publication_id: str) -> Dict:
        return self.create_batch_job_message(
            resource_id=publication_id,
            job_type=BatchJobType.REINDEX_RECORD
        )
    
    def send_batch(self, messages: List[Dict], queue_url: str) -> Tuple[int, int, List[Dict]]:
        """
        Send a batch of messages to SQS.
        
        Args:
            messages: List of message dictionaries
            queue_url: The SQS queue URL
            
        Returns:
            Tuple of (successful_count, failed_count, failed_messages)
        """
        try:
            # Format messages for SQS batch send
            sqs_messages = [
                {
                    "Id": str(i),
                    "MessageBody": json.dumps(msg)
                }
                for i, msg in enumerate(messages)
            ]
            
            response = self.sqs.send_message_batch(
                QueueUrl=queue_url,
                Entries=sqs_messages
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
        parameters: Optional[Dict] = None
    ) -> Dict:
        # Find the queue
        queue_url = self.find_batch_job_queue()
        if not queue_url:
            return {
                "success": False,
                "error": "No DynamodbResourceBatchJobWorkQueue found",
                "total_processed": 0,
                "successful": 0,
                "failed": 0
            }
        
        # Read resource IDs from file
        with open(input_file, "r") as f:
            resource_ids = [line.strip() for line in f if line.strip()]
        
        total_ids = len(resource_ids)
        total_sent = 0
        failed_count = 0
        all_failures = []
        batch_messages = []
        
        for i, resource_id in enumerate(resource_ids, 1):
            # Create message for this resource
            message = self.create_batch_job_message(
                resource_id=resource_id,
                job_type=job_type,
                parameters=parameters
            )
            batch_messages.append(message)
            
            # Send batch when it reaches the specified size or at the end
            if len(batch_messages) >= batch_size or i == total_ids:
                successful, failed, failures = self.send_batch(
                    batch_messages,
                    queue_url
                )
                
                total_sent += successful
                failed_count += failed
                all_failures.extend(failures)
                
                # Clear the batch for next iteration
                batch_messages = []
                
                # Small delay to avoid throttling
                if i < total_ids:
                    time.sleep(0.1)
        
        return {
            "success": total_sent == total_ids,
            "queue_url": queue_url,
            "job_type": job_type.value,
            "total_processed": total_ids,
            "successful": total_sent,
            "failed": failed_count,
            "failures": all_failures
        }
    
    def process_reindex_job(self, input_file: str, batch_size: int = 10) -> Dict:
        """
        Convenience method for processing a reindex job.
        
        Args:
            input_file: Path to file containing publication IDs
            batch_size: Number of messages to send per batch
            
        Returns:
            Dictionary with job results
        """
        return self.process_batch_job(
            input_file=input_file,
            job_type=BatchJobType.REINDEX_RECORD,
            batch_size=batch_size
        )