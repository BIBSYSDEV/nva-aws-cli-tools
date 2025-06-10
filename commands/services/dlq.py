from collections import defaultdict


def get_messages(sqs_client, queue: str, max_count: int) -> list[dict[str, any]]:
    """Read messages from queue."""
    print("Reading messages from the queue...")
    all_messages: list[dict[str, any]] = []
    seen_message_ids: set[str] = set()

    while len(all_messages) < max_count:
        response = sqs_client.receive_message(
            QueueUrl=queue,
            MaxNumberOfMessages=min(10, max_count - len(all_messages)),
            WaitTimeSeconds=1,
            MessageAttributeNames=["All"],
            AttributeNames=["All"],
        )

        messages = response.get("Messages", [])
        if not messages:
            break

        new_messages = [
            msg for msg in messages if msg["MessageId"] not in seen_message_ids
        ]
        if not new_messages:
            break

        all_messages.extend(new_messages)
        seen_message_ids.update(msg["MessageId"] for msg in new_messages)
        print(f"Received {len(new_messages)} new messages.")

    print(f"Total messages read: {len(all_messages)}")
    return all_messages


def summarize_messages(messages: list[dict[str, any]]) -> tuple:
    """Summarize messages by sender and body."""
    by_sender = defaultdict(lambda: {"count": 0, "candidates": set()})
    by_type = defaultdict(lambda: {"count": 0, "candidates": set()})

    for msg in messages:
        sender_id = msg.get("Attributes", {}).get("SenderId", "Unknown")
        body = msg.get("Body", "")[:50]
        candidate = (
            msg.get("MessageAttributes", {})
            .get("candidateIdentifier", {})
            .get("StringValue", "Unknown")
        )

        by_sender[sender_id]["count"] += 1
        by_sender[sender_id]["candidates"].add(candidate)

        by_type[body]["count"] += 1
        by_type[body]["candidates"].add(candidate)

    return by_sender, by_type


def delete_messages_with_prefix(
    sqs_client, queue: str, prefix: str, max_count: int
) -> int:
    """Delete messages with specific prefix in body as we encounter them."""
    print(f"Deleting messages with prefix '{prefix}'...")
    deleted_count = 0
    processed_count = 0
    seen_message_ids: set[str] = set()

    while processed_count < max_count:
        response = sqs_client.receive_message(
            QueueUrl=queue,
            MaxNumberOfMessages=min(10, max_count - processed_count),
            WaitTimeSeconds=1,
            MessageAttributeNames=["All"],
            AttributeNames=["All"],
        )

        messages = response.get("Messages", [])
        if not messages:
            break

        new_messages = [
            msg for msg in messages if msg["MessageId"] not in seen_message_ids
        ]
        if not new_messages:
            break

        seen_message_ids.update(msg["MessageId"] for msg in new_messages)
        processed_count += len(new_messages)

        # Process each message
        for msg in new_messages:
            body = msg.get("Body", "")
            if body.startswith(prefix):
                try:
                    print(f"Deleting message: {msg['MessageId']} - {body[:50]}...")
                    sqs_client.delete_message(
                        QueueUrl=queue, ReceiptHandle=msg["ReceiptHandle"]
                    )
                    deleted_count += 1
                except Exception as e:
                    print(f"Failed to delete message {msg['MessageId']}: {e}")
            else:
                # Reset visibility for non-matching messages
                try:
                    sqs_client.change_message_visibility(
                        QueueUrl=queue,
                        ReceiptHandle=msg["ReceiptHandle"],
                        VisibilityTimeout=0,
                    )
                except Exception as e:
                    print(
                        f"Warning: Could not reset visibility for message {msg['MessageId']}: {e}"
                    )

    return deleted_count
