from collections import defaultdict


def get_messages(sqs_client, queue: str, max_count: int) -> None:
    print("Reading all messages from the queue...")
    all_messages: list[dict] = []
    seen_message_ids: set = set()
    while True and len(all_messages) < max_count:
        print("Fetching batch of messages from the queue...")
        response = sqs_client.receive_message(
            QueueUrl=queue,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,  # Short polling
            MessageAttributeNames=["All"],
            AttributeNames=["All"],
        )

        messages = response.get("Messages", [])
        new_messages = [
            msg for msg in messages if msg["MessageId"] not in seen_message_ids
        ]
        if not new_messages:
            break

        all_messages.extend(new_messages)
        seen_message_ids.update(msg["MessageId"] for msg in new_messages)
        all_messages.extend(new_messages)
        print(f"Received {len(new_messages)} messages.")

        # Change visibility timeout to 0 to make messages immediately available again
        receipt_handles = [msg["ReceiptHandle"] for msg in new_messages]
        for handle in receipt_handles:
            sqs_client.change_message_visibility(
                QueueUrl=queue, ReceiptHandle=handle, VisibilityTimeout=0
            )

    print(f"Total messages read: {len(all_messages)}")
    return all_messages


def summarize_messages(messages: list[dict]) -> dict:
    """Summarize messages by sender and body."""
    summary = defaultdict(
        lambda: defaultdict(lambda: {"count": 0, "candidates": set()})
    )
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
        summary[sender_id][body]["count"] += 1
        summary[sender_id][body]["candidates"].add(candidate)

        by_sender[sender_id]["count"] += 1
        by_sender[sender_id]["candidates"].add(candidate)

        by_type[body]["count"] += 1
        by_type[body]["candidates"].add(candidate)
    return by_sender, by_type
