import json
import re
import signal
import threading
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    MofNCompleteColumn,
    TaskID,
)
from rich.table import Table

console = Console()

# Constants
MAX_EMPTY_RECEIVES = 3
SQS_MAX_BATCH_SIZE = 10
THREAD_JOIN_TIMEOUT_SECONDS = 10
LONG_POLL_WAIT_SECONDS = 20
SHORT_POLL_WAIT_SECONDS = 0
DEFAULT_THREADS = 5
MAX_MESSAGES_PER_FILE = 1000

# Compiled regex patterns for analysis
EXCEPTION_PATTERNS = [
    (re.compile(r"([A-Z]\w*Exception)"), "exception_types"),
    (re.compile(r"([A-Z]\w*Error)"), "error_types"),
    (re.compile(r"(java\.\w+(?:\.\w+)*?[A-Z]\w*Exception)"), "java_exceptions"),
    (re.compile(r"(com\.\w+(?:\.\w+)*?[A-Z]\w*Exception)"), "custom_exceptions"),
    (re.compile(r"ERROR.*?:(.*?)(?:\n|$)"), "error_messages"),
    (re.compile(r"failed to ([\w\s]+)"), "failure_reasons"),
    (re.compile(r"Unable to ([\w\s]+)"), "unable_to"),
    (re.compile(r"Cannot ([\w\s]+)"), "cannot_do"),
    (re.compile(r"Missing ([\w\s]+)"), "missing_items"),
    (re.compile(r"Invalid ([\w\s]+)"), "invalid_items"),
]

EXCEPTION_CONTEXT_PATTERNS = [
    re.compile(
        r"([A-Z]\w*(?:Exception|Error))\[([^\]]{1,200})\](?:; nested: ([^;]{1,100}))?"
    ),
    re.compile(r"([A-Z]\w*(?:Exception|Error)):\s*([^\n]{1,200})"),
    re.compile(r"Caused by:\s*([^:\n]+):\s*([^\n]{1,200})"),
]


class SqsService:
    def __init__(self, profile: Optional[str] = None):
        if profile:
            self.session = boto3.Session(profile_name=profile)
        else:
            self.session = boto3.Session()
        self.sqs_client = self.session.client("sqs")
        self.profile = profile or "default"

    def find_queue_url(self, queue_name_partial: str) -> Optional[str]:
        try:
            response = self.sqs_client.list_queues()
            queue_urls = response.get("QueueUrls", [])

            matching_queues = []
            for url in queue_urls:
                queue_name = url.split("/")[-1]
                if queue_name_partial.lower() in queue_name.lower():
                    matching_queues.append(url)

            if not matching_queues:
                console.print(
                    f"[red]No queues found matching '{queue_name_partial}'[/red]"
                )
                return None

            if len(matching_queues) == 1:
                queue_name = matching_queues[0].split("/")[-1]
                console.print(f"[green]Found queue: {queue_name}[/green]")
                return matching_queues[0]

            table = Table(title="Multiple queues found")
            table.add_column("Index", style="cyan")
            table.add_column("Queue Name", style="yellow")

            for i, url in enumerate(matching_queues):
                queue_name = url.split("/")[-1]
                table.add_row(str(i + 1), queue_name)

            console.print(table)

            while True:
                choice = console.input(
                    "[cyan]Select queue by index (or 'q' to quit): [/cyan]"
                )
                if choice.lower() == "q":
                    return None
                try:
                    index = int(choice) - 1
                    if 0 <= index < len(matching_queues):
                        return matching_queues[index]
                    console.print("[red]Invalid index. Please try again.[/red]")
                except ValueError:
                    console.print("[red]Please enter a number or 'q' to quit.[/red]")

        except ClientError as e:
            console.print(f"[red]Error listing queues: {e}[/red]")
            raise e

    def get_queue_attributes(self, queue_url: str) -> Dict[str, Any]:
        try:
            response = self.sqs_client.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["All"]
            )
            return response.get("Attributes", {})
        except ClientError as e:
            console.print(f"[red]Error getting queue attributes: {e}[/red]")
            raise e

    def receive_messages(
        self, queue_url: str, max_messages: int = 10
    ) -> List[Dict[str, Any]]:
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=min(max_messages, SQS_MAX_BATCH_SIZE),
                MessageAttributeNames=["All"],
                AttributeNames=["All"],
                WaitTimeSeconds=LONG_POLL_WAIT_SECONDS,
            )

            messages = response.get("Messages", [])

            processed_messages = []
            for msg in messages:
                processed_msg = {
                    "MessageId": msg.get("MessageId"),
                    "ReceiptHandle": msg.get("ReceiptHandle"),
                    "Body": msg.get("Body"),
                    "Attributes": msg.get("Attributes", {}),
                    "MessageAttributes": msg.get("MessageAttributes", {}),
                    "MD5OfBody": msg.get("MD5OfBody"),
                    "MD5OfMessageAttributes": msg.get("MD5OfMessageAttributes"),
                }

                try:
                    processed_msg["ParsedBody"] = json.loads(msg.get("Body", "{}"))
                except (json.JSONDecodeError, TypeError):
                    processed_msg["ParsedBody"] = None

                processed_messages.append(processed_msg)

            return processed_messages

        except ClientError as e:
            console.print(f"[red]Error receiving messages: {e}[/red]")
            return []

    def delete_message(self, queue_url: str, receipt_handle: str) -> None:
        self.sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    def delete_message_batch(self, queue_url: str, receipt_handles: List[str]) -> int:
        if not receipt_handles:
            return 0

        deleted_count = 0
        for i in range(0, len(receipt_handles), 10):
            batch = receipt_handles[i : i + 10]
            entries = [
                {"Id": str(j), "ReceiptHandle": handle}
                for j, handle in enumerate(batch)
            ]

            try:
                response = self.sqs_client.delete_message_batch(
                    QueueUrl=queue_url, Entries=entries
                )
                deleted_count += len(response.get("Successful", []))

                failed = response.get("Failed", [])
                if failed:
                    for failure in failed:
                        console.print(
                            f"[yellow]Failed to delete message: {failure.get('Message')}[/yellow]"
                        )

            except ClientError as e:
                console.print(f"[red]Error deleting message batch: {e}[/red]")

        return deleted_count

    def _drain_worker_thread(
        self,
        queue_url: str,
        base_dir: Path,
        max_messages_per_file: int,
        delete_after_write: bool,
        stop_event: threading.Event,
        stats: Dict[str, Any],
    ) -> None:
        sqs_client = self.session.client("sqs")

        messages_buffer = []
        receipt_handles_buffer = []
        consecutive_empty = 0

        while not stop_event.is_set() and consecutive_empty < MAX_EMPTY_RECEIVES:
            try:
                response = sqs_client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=SQS_MAX_BATCH_SIZE,
                    MessageAttributeNames=["All"],
                    AttributeNames=["All"],
                    WaitTimeSeconds=SHORT_POLL_WAIT_SECONDS,
                )

                messages = response.get("Messages", [])

                if not messages:
                    consecutive_empty += 1
                    if consecutive_empty >= 3 and messages_buffer:
                        with stats["lock"]:
                            stats["file_counter"] += 1
                            file_num = stats["file_counter"]

                        output_file = base_dir / f"messages_{file_num:04d}.jsonl"
                        content = "".join(
                            json.dumps(
                                {k: v for k, v in msg.items() if k != "ReceiptHandle"},
                                default=str,
                            )
                            + "\n"
                            for msg in messages_buffer
                        )
                        output_file.write_text(content)

                        with stats["lock"]:
                            stats["written"] += len(messages_buffer)

                        if delete_after_write and receipt_handles_buffer:
                            deleted = self.delete_message_batch(
                                queue_url, receipt_handles_buffer
                            )
                            with stats["lock"]:
                                stats["deleted"] += deleted

                        messages_buffer = []
                        receipt_handles_buffer = []
                    continue

                consecutive_empty = 0

                for msg in messages:
                    processed_msg = {
                        "MessageId": msg.get("MessageId"),
                        "ReceiptHandle": msg.get("ReceiptHandle"),
                        "Body": msg.get("Body"),
                        "Attributes": msg.get("Attributes", {}),
                        "MessageAttributes": msg.get("MessageAttributes", {}),
                        "MD5OfBody": msg.get("MD5OfBody"),
                        "MD5OfMessageAttributes": msg.get("MD5OfMessageAttributes"),
                    }

                    try:
                        processed_msg["ParsedBody"] = json.loads(msg.get("Body", "{}"))
                    except (json.JSONDecodeError, TypeError):
                        processed_msg["ParsedBody"] = None

                    messages_buffer.append(processed_msg)
                    receipt_handles_buffer.append(msg["ReceiptHandle"])

                    with stats["lock"]:
                        stats["received"] += 1

                    if len(messages_buffer) >= max_messages_per_file:
                        with stats["lock"]:
                            stats["file_counter"] += 1
                            file_num = stats["file_counter"]

                        output_file = base_dir / f"messages_{file_num:04d}.jsonl"
                        content = "".join(
                            json.dumps(
                                {k: v for k, v in msg.items() if k != "ReceiptHandle"},
                                default=str,
                            )
                            + "\n"
                            for msg in messages_buffer
                        )
                        output_file.write_text(content)

                        with stats["lock"]:
                            stats["written"] += len(messages_buffer)

                        if delete_after_write:
                            deleted = self.delete_message_batch(
                                queue_url, receipt_handles_buffer
                            )
                            with stats["lock"]:
                                stats["deleted"] += deleted

                        messages_buffer = []
                        receipt_handles_buffer = []

            except Exception as e:
                if stop_event.is_set():
                    # Save any remaining messages before exiting
                    if messages_buffer:
                        with stats["lock"]:
                            stats["remaining_messages"].extend(messages_buffer)
                            stats["remaining_receipts"].extend(receipt_handles_buffer)
                    break
                console.print(f"[red]Error in worker thread: {e}[/red]")
                with stats["lock"]:
                    stats["errors"] = stats.get("errors", 0) + 1

        # Thread is ending - save any remaining messages
        if messages_buffer:
            with stats["lock"]:
                stats["remaining_messages"].extend(messages_buffer)
                stats["remaining_receipts"].extend(receipt_handles_buffer)

    def drain_queue(
        self,
        queue_url: str,
        output_dir: Optional[str] = None,
        max_messages_per_file: int = 1000,
        delete_after_write: bool = True,
        num_threads: int = 5,
    ) -> bool:
        if not queue_url:
            return False

        queue_name = queue_url.split("/")[-1]

        if num_threads == 1:
            console.print("[cyan]Using single-threaded mode[/cyan]")
            return self._drain_single_thread(
                queue_url,
                queue_name,
                output_dir,
                max_messages_per_file,
                delete_after_write,
            )

        console.print(f"[cyan]Using {num_threads} parallel threads[/cyan]")

        queue_attrs = self.get_queue_attributes(queue_url)
        if queue_attrs:
            approx_messages = int(queue_attrs.get("ApproximateNumberOfMessages", 0))
            console.print(
                f"[cyan]Queue has approximately {approx_messages} messages[/cyan]"
            )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if output_dir:
            base_dir = Path(output_dir)
        else:
            base_dir = Path(f"{self.profile}-{queue_name}-{timestamp}")

        base_dir.mkdir(parents=True, exist_ok=True)
        console.print(f"[green]Output directory: {base_dir}[/green]")

        metadata_file = base_dir / "metadata.json"
        metadata_file.write_text(
            json.dumps(
                {
                    "queue_url": queue_url,
                    "queue_name": queue_name,
                    "profile": self.profile,
                    "timestamp": timestamp,
                    "attributes": queue_attrs or {},
                    "num_threads": num_threads,
                },
                indent=2,
                default=str,
            )
        )

        stats = {
            "received": 0,
            "written": 0,
            "deleted": 0,
            "errors": 0,
            "file_counter": 0,
            "lock": threading.Lock(),
            "remaining_messages": [],
            "remaining_receipts": [],
        }

        stop_event = threading.Event()

        def signal_handler(_signum, _frame):
            console.print(
                "\n[yellow]Stopping... Please wait for threads to finish current batch[/yellow]"
            )
            stop_event.set()

        old_handler = signal.signal(signal.SIGINT, signal_handler)

        try:
            threads = []
            for _ in range(num_threads):
                thread = threading.Thread(
                    target=self._drain_worker_thread,
                    args=(
                        queue_url,
                        base_dir,
                        max_messages_per_file,
                        delete_after_write,
                        stop_event,
                        stats,
                    ),
                    daemon=False,
                )
                thread.start()
                threads.append(thread)

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task(
                    f"[cyan]Draining queue {queue_name}...", total=None
                )

                last_written = 0
                while any(t.is_alive() for t in threads):
                    current_written = stats["written"]
                    if current_written != last_written or stats["received"] % 100 == 0:
                        progress.update(
                            task,
                            description=f"[cyan]Draining... (received: {stats['received']}, "
                            f"written: {stats['written']}, files: {stats['file_counter']})",
                        )
                        last_written = current_written

                    threading.Event().wait(0.1)

                    if stop_event.is_set():
                        break

            for thread in threads:
                thread.join(timeout=THREAD_JOIN_TIMEOUT_SECONDS)
                if thread.is_alive():
                    console.print(
                        "[yellow]Warning: Worker thread did not finish cleanly[/yellow]"
                    )

            # Write any remaining messages from all threads
            if stats["remaining_messages"]:
                with stats["lock"]:
                    stats["file_counter"] += 1
                    file_num = stats["file_counter"]

                output_file = base_dir / f"messages_{file_num:04d}.jsonl"
                content = "".join(
                    json.dumps(
                        {k: v for k, v in msg.items() if k != "ReceiptHandle"},
                        default=str,
                    )
                    + "\n"
                    for msg in stats["remaining_messages"]
                )
                output_file.write_text(content)

                stats["written"] += len(stats["remaining_messages"])

                if delete_after_write and stats["remaining_receipts"]:
                    try:
                        deleted = self.delete_message_batch(
                            queue_url, stats["remaining_receipts"]
                        )
                        stats["deleted"] += deleted
                    except Exception as e:
                        console.print(f"[red]Error deleting final batch: {e}[/red]")

        finally:
            signal.signal(signal.SIGINT, old_handler)

        summary_file = base_dir / "summary.json"
        summary_file.write_text(
            json.dumps(
                {
                    "queue_name": queue_name,
                    "profile": self.profile,
                    "total_messages": stats["written"],
                    "files_created": stats["file_counter"],
                    "messages_deleted": delete_after_write,
                    "messages_deleted_count": stats["deleted"],
                    "num_threads": num_threads,
                    "timestamp_start": timestamp,
                    "timestamp_end": datetime.now().strftime("%Y%m%d_%H%M%S"),
                },
                indent=2,
            )
        )

        console.print(
            f"\n[bold green]✓ Drained {stats['written']} messages from {queue_name}[/bold green]"
        )
        console.print(f"[cyan]Output: {base_dir}[/cyan]")
        console.print(f"[cyan]Files created: {stats['file_counter']}[/cyan]")
        if delete_after_write:
            console.print(f"[cyan]Messages deleted: {stats['deleted']}[/cyan]")
        if stats.get("errors", 0) > 0:
            console.print(
                f"[yellow]Warning: {stats['errors']} errors occurred during processing[/yellow]"
            )

        return True

    def _drain_single_thread(
        self,
        queue_url: str,
        queue_name: str,
        output_dir: Optional[str],
        max_messages_per_file: int,
        delete_after_write: bool,
    ) -> bool:
        queue_attrs = self.get_queue_attributes(queue_url)
        if queue_attrs:
            approx_messages = int(queue_attrs.get("ApproximateNumberOfMessages", 0))
            console.print(
                f"[cyan]Queue has approximately {approx_messages} messages[/cyan]"
            )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if output_dir:
            base_dir = Path(output_dir)
        else:
            base_dir = Path(f"{self.profile}-{queue_name}-{timestamp}")

        base_dir.mkdir(parents=True, exist_ok=True)
        console.print(f"[green]Output directory: {base_dir}[/green]")

        metadata_file = base_dir / "metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(
                {
                    "queue_url": queue_url,
                    "queue_name": queue_name,
                    "profile": self.profile,
                    "timestamp": timestamp,
                    "attributes": queue_attrs or {},
                },
                f,
                indent=2,
                default=str,
            )

        total_messages = 0
        file_count = 0
        messages_buffer = []
        receipt_handles_buffer = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"[cyan]Draining queue {queue_name}...", total=None
            )

            consecutive_empty_receives = 0

            while consecutive_empty_receives < MAX_EMPTY_RECEIVES:
                messages = self.receive_messages(
                    queue_url, max_messages=SQS_MAX_BATCH_SIZE
                )

                if not messages:
                    consecutive_empty_receives += 1
                    continue

                consecutive_empty_receives = 0

                for msg in messages:
                    messages_buffer.append(msg)
                    receipt_handles_buffer.append(msg["ReceiptHandle"])
                    total_messages += 1

                    if len(messages_buffer) >= max_messages_per_file:
                        file_count += 1
                        output_file = base_dir / f"messages_{file_count:04d}.jsonl"

                        with open(output_file, "w") as f:
                            for buffered_msg in messages_buffer:
                                saved_msg = {
                                    k: v
                                    for k, v in buffered_msg.items()
                                    if k != "ReceiptHandle"
                                }
                                f.write(json.dumps(saved_msg, default=str) + "\n")

                        console.print(
                            f"[green]Wrote {len(messages_buffer)} messages to {output_file.name}[/green]"
                        )

                        if delete_after_write:
                            deleted = self.delete_message_batch(
                                queue_url, receipt_handles_buffer
                            )
                            if deleted != len(receipt_handles_buffer):
                                console.print(
                                    f"[yellow]Warning: Only deleted {deleted}/{len(receipt_handles_buffer)} messages[/yellow]"
                                )

                        messages_buffer = []
                        receipt_handles_buffer = []

                progress.update(
                    task,
                    description=f"[cyan]Draining queue {queue_name}... ({total_messages} messages processed)",
                )

            if messages_buffer:
                file_count += 1
                output_file = base_dir / f"messages_{file_count:04d}.jsonl"

                with open(output_file, "w") as f:
                    for buffered_msg in messages_buffer:
                        saved_msg = {
                            k: v
                            for k, v in buffered_msg.items()
                            if k != "ReceiptHandle"
                        }
                        f.write(json.dumps(saved_msg, default=str) + "\n")

                console.print(
                    f"[green]Wrote {len(messages_buffer)} messages to {output_file.name}[/green]"
                )

                if delete_after_write:
                    deleted = self.delete_message_batch(
                        queue_url, receipt_handles_buffer
                    )
                    if deleted != len(receipt_handles_buffer):
                        console.print(
                            f"[yellow]Warning: Only deleted {deleted}/{len(receipt_handles_buffer)} messages[/yellow]"
                        )

        summary_file = base_dir / "summary.json"
        with open(summary_file, "w") as f:
            json.dump(
                {
                    "queue_name": queue_name,
                    "profile": self.profile,
                    "total_messages": total_messages,
                    "files_created": file_count,
                    "messages_deleted": delete_after_write,
                    "timestamp_start": timestamp,
                    "timestamp_end": datetime.now().strftime("%Y%m%d_%H%M%S"),
                },
                f,
                indent=2,
            )

        console.print(
            f"\n[bold green]✓ Drained {total_messages} messages from {queue_name}[/bold green]"
        )
        console.print(f"[cyan]Output: {base_dir}[/cyan]")
        console.print(f"[cyan]Files created: {file_count}[/cyan]")

        return True

    def analyze_drained_messages(self, folder_path: str) -> Dict[str, Any]:
        folder = Path(folder_path)
        if not folder.exists() or not folder.is_dir():
            console.print(f"[red]Directory not found: {folder_path}[/red]")
            return {}

        console.print(f"[cyan]Analyzing messages in: {folder}[/cyan]\n")

        total_messages = 0
        exception_types = Counter()
        message_types = Counter()
        attribute_keys = Counter()
        message_attribute_keys = Counter()
        common_patterns = defaultdict(int)
        stack_traces = []
        exception_contexts = Counter()

        jsonl_files = list(folder.glob("messages_*.jsonl"))
        if not jsonl_files:
            console.print(
                "[yellow]No message files found (looking for messages_*.jsonl)[/yellow]"
            )
            return {}

        console.print(f"[green]Found {len(jsonl_files)} message files[/green]\n")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(
                "[cyan]Analyzing messages...", total=len(jsonl_files)
            )

            for file_path in sorted(jsonl_files):
                with open(file_path, "r") as f:
                    for line_num, line in enumerate(f, 1):
                        try:
                            msg = json.loads(line)
                            total_messages += 1
                            body = msg.get("Body", "")
                            parsed_body = msg.get("ParsedBody")
                            if parsed_body:
                                body_text = json.dumps(parsed_body)
                            else:
                                body_text = str(body)

                            seen_patterns_in_msg = set()
                            for pattern, pattern_name in EXCEPTION_PATTERNS:
                                matches = pattern.findall(body_text)
                                for match in matches:
                                    if isinstance(match, tuple):
                                        match = match[0]

                                    match = match.strip()[:100]
                                    if match:
                                        if pattern_name in [
                                            "exception_types",
                                            "error_types",
                                            "java_exceptions",
                                            "custom_exceptions",
                                        ]:
                                            base_match = (
                                                match.split(".")[-1]
                                                if "." in match
                                                else match
                                            )
                                            pattern_key = f"{pattern_name}:{base_match}"
                                        else:
                                            pattern_key = f"{pattern_name}:{match}"
                                        if pattern_key not in seen_patterns_in_msg:
                                            common_patterns[pattern_key] += 1
                                            seen_patterns_in_msg.add(pattern_key)
                            if (
                                "exception" in body_text.lower()
                                or "error" in body_text.lower()
                            ):
                                exc_matches = re.findall(
                                    r"([A-Z][a-zA-Z]*(?:Exception|Error))", body_text
                                )
                                seen_in_message = set()
                                for exc in exc_matches:
                                    base_exc = exc.split(".")[-1] if "." in exc else exc
                                    if base_exc not in seen_in_message:
                                        exception_types[base_exc] += 1
                                        seen_in_message.add(base_exc)

                                for pattern in EXCEPTION_CONTEXT_PATTERNS:
                                    context_matches = pattern.findall(body_text)
                                    for match in context_matches:
                                        if isinstance(match, tuple) and len(match) >= 2:
                                            if match[0] and match[1]:
                                                context = f"{match[0]}: {match[1][:150].strip()}"
                                                if len(match) > 2 and match[2]:
                                                    context += (
                                                        f" [nested: {match[2][:50]}]"
                                                    )
                                                exception_contexts[context] += 1
                                if "\tat " in body_text or "Traceback" in body_text:
                                    stack_traces.append(
                                        {
                                            "file": file_path.name,
                                            "line": line_num,
                                            "preview": body_text[:200],
                                        }
                                    )
                            if "Attributes" in msg:
                                for key in msg["Attributes"].keys():
                                    attribute_keys[key] += 1
                            if "MessageAttributes" in msg:
                                for key in msg["MessageAttributes"].keys():
                                    message_attribute_keys[key] += 1
                            if parsed_body is not None:
                                if isinstance(parsed_body, dict):
                                    if "eventType" in parsed_body:
                                        message_types[parsed_body["eventType"]] += 1
                                    elif "type" in parsed_body:
                                        message_types[parsed_body["type"]] += 1
                                    elif "action" in parsed_body:
                                        message_types[
                                            f"action:{parsed_body['action']}"
                                        ] += 1
                                    elif "error" in parsed_body:
                                        message_types["error_message"] += 1
                                    else:
                                        message_types["generic_json"] += 1
                                elif isinstance(
                                    parsed_body, (list, str, int, float, bool)
                                ):
                                    message_types[
                                        f"json_{type(parsed_body).__name__}"
                                    ] += 1
                                else:
                                    message_types["json_other"] += 1
                            else:
                                if (
                                    "exception" in body.lower()
                                    or "error" in body.lower()
                                ):
                                    message_types["plain_text_error"] += 1
                                elif body.strip().startswith("<?xml"):
                                    message_types["xml"] += 1
                                else:
                                    message_types["plain_text"] += 1

                        except json.JSONDecodeError:
                            console.print(
                                f"[yellow]Warning: Invalid JSON in {file_path.name}:{line_num}[/yellow]"
                            )
                        except Exception as e:
                            console.print(
                                f"[red]Error processing {file_path.name}:{line_num}: {e}[/red]"
                            )

                progress.update(task, advance=1)
        console.print("\n" + "=" * 60)
        console.print("[bold cyan]Message Analysis Results[/bold cyan]")
        console.print("=" * 60 + "\n")

        console.print(f"[bold]Total Messages:[/bold] {total_messages:,}\n")

        if total_messages == 0:
            console.print("[yellow]No messages to analyze[/yellow]")
            return {
                "total_messages": 0,
                "exception_types": {},
                "exception_contexts": {},
                "message_types": {},
                "common_patterns": {},
                "stack_trace_count": 0,
                "attribute_keys": {},
                "message_attribute_keys": {},
            }

        all_exceptions = {}
        for exc_type, count in exception_types.items():
            all_exceptions[exc_type] = count
        for pattern_key, count in common_patterns.items():
            if "exception" in pattern_key.lower() or "error" in pattern_key.lower():
                pattern_type, pattern_value = pattern_key.split(":", 1)
                if "Exception" in pattern_value or "Error" in pattern_value:
                    base_name = (
                        pattern_value.split(".")[-1]
                        if "." in pattern_value
                        else pattern_value
                    )
                    if base_name not in all_exceptions and (
                        base_name.endswith("Exception") or base_name.endswith("Error")
                    ):
                        all_exceptions[base_name] = count
        if all_exceptions:
            table = Table(
                title="Exceptions and Errors (Messages Containing)", show_header=True
            )
            table.add_column("Exception/Error", style="red")
            table.add_column("Messages", style="yellow", justify="right")
            table.add_column("% of Total", style="green", justify="right")
            sorted_exceptions = sorted(
                all_exceptions.items(), key=lambda x: x[1], reverse=True
            )
            for exc_type, count in sorted_exceptions[:20]:
                percentage = (count / total_messages) * 100

                percentage = min(percentage, 100.0)
                table.add_row(exc_type, f"{count:,}", f"{percentage:.1f}%")
            console.print(table)
            console.print()
        if exception_contexts:
            recurring_contexts = [
                (ctx, count) for ctx, count in exception_contexts.items() if count > 1
            ]

            if recurring_contexts:
                recurring_contexts.sort(key=lambda x: x[1], reverse=True)

                table = Table(
                    title="Recurring Exception Patterns (Specific Error Messages)",
                    show_header=True,
                )
                table.add_column("Exception Pattern", style="yellow", max_width=120)
                table.add_column("Count", style="red", justify="right")
                table.add_column("% of Msgs", style="green", justify="right")
                for context, count in recurring_contexts[:25]:
                    percentage = (count / total_messages) * 100

                    display_context = context
                    table.add_row(display_context, str(count), f"{percentage:.1f}%")

                console.print(table)
                console.print()
        meaningful_patterns = defaultdict(list)
        for pattern_key, count in common_patterns.items():
            pattern_type, pattern_value = pattern_key.split(":", 1)

            if pattern_type not in [
                "exception_types",
                "error_types",
                "java_exceptions",
                "custom_exceptions",
                "error_messages",
            ]:
                if pattern_type in [
                    "failure_reasons",
                    "cannot_do",
                    "missing_items",
                    "invalid_items",
                ]:
                    meaningful_patterns[pattern_type].append((pattern_value, count))
        if meaningful_patterns:
            all_failures = []
            for pattern_type, patterns in meaningful_patterns.items():
                for pattern, count in patterns:
                    all_failures.append(
                        (pattern_type.replace("_", " ").title(), pattern, count)
                    )

            if all_failures:
                all_failures.sort(key=lambda x: x[2], reverse=True)
                table = Table(title="Failure Patterns", show_header=True)
                table.add_column("Type", style="cyan")
                table.add_column("Pattern", style="yellow", max_width=50)
                table.add_column("Count", style="magenta", justify="right")

                for failure_type, pattern, count in all_failures[:15]:
                    table.add_row(failure_type, pattern, str(count))
                console.print(table)
                console.print()
        if attribute_keys:
            table = Table(title="SQS Attributes Used", show_header=True)
            table.add_column("Attribute", style="magenta")
            table.add_column("Messages", style="yellow", justify="right")

            for attr, count in attribute_keys.most_common():
                table.add_row(attr, str(count))
            console.print(table)
            console.print()
        if message_attribute_keys:
            table = Table(title="Message Attributes Used", show_header=True)
            table.add_column("Attribute", style="blue")
            table.add_column("Messages", style="yellow", justify="right")

            for attr, count in message_attribute_keys.most_common():
                table.add_row(attr, str(count))
            console.print(table)
            console.print()
        unique_types = len(message_types)
        if unique_types > 1:
            table = Table(title="Message Type Distribution", show_header=True)
            table.add_column("Type", style="cyan")
            table.add_column("Count", style="yellow", justify="right")
            table.add_column("Percentage", style="green", justify="right")

            for msg_type, count in message_types.most_common(10):
                percentage = (count / total_messages) * 100

                display_type = msg_type.replace("_", " ").title()
                if msg_type.startswith("json_"):
                    display_type = f"JSON ({msg_type[5:]})"
                elif msg_type.startswith("action:"):
                    display_type = f"Action: {msg_type[7:]}"
                table.add_row(display_type, str(count), f"{percentage:.1f}%")
            console.print(table)
            console.print()
        if stack_traces:
            percentage_with_traces = (len(stack_traces) / total_messages) * 100
            console.print(
                f"[bold yellow]Stack Traces:[/bold yellow] {len(stack_traces):,} messages ({percentage_with_traces:.1f}%) contain stack traces"
            )

            if len(stack_traces) <= 10:
                console.print("[dim]Locations:[/dim]")
                for trace_info in stack_traces:
                    console.print(f"  • {trace_info['file']}:{trace_info['line']}")
            else:
                console.print("[dim]Sample locations (first 5):[/dim]")
                for trace_info in stack_traces[:5]:
                    console.print(f"  • {trace_info['file']}:{trace_info['line']}")
                    preview = trace_info["preview"][:100].replace("\n", " ")
                    console.print(f"    [dim]{preview}...[/dim]")
            console.print()
        console.print("[bold cyan]Summary Statistics:[/bold cyan]")
        if all_exceptions:
            top_exception = sorted_exceptions[0] if sorted_exceptions else ("None", 0)
            console.print(
                f"  • Most common exception: [red]{top_exception[0]}[/red] ({top_exception[1]:,} occurrences)"
            )

        if stack_traces:
            console.print(
                f"  • Messages with stack traces: {len(stack_traces):,} ({percentage_with_traces:.1f}%)"
            )
        if message_types and unique_types > 1:
            dominant_type = message_types.most_common(1)[0]
            dominant_percentage = (dominant_type[1] / total_messages) * 100

            if dominant_percentage < 95:
                console.print(
                    f"  • Dominant message type: {dominant_type[0]} ({dominant_percentage:.1f}%)"
                )

        console.print()
        if len(common_patterns) > 10 and total_messages > 100:
            self._find_common_substrings(common_patterns)
        return {
            "total_messages": total_messages,
            "exception_types": dict(exception_types),
            "exception_contexts": dict(exception_contexts),
            "message_types": dict(message_types),
            "common_patterns": dict(common_patterns),
            "stack_trace_count": len(stack_traces),
            "attribute_keys": dict(attribute_keys),
            "message_attribute_keys": dict(message_attribute_keys),
        }

    def _find_common_substrings(
        self, patterns: Dict[str, int], min_length: int = 20
    ) -> None:
        pattern_texts = list(patterns.keys())
        if len(pattern_texts) < 2:
            return

        common_substrings = Counter()
        for i in range(len(pattern_texts)):
            for j in range(i + 1, min(i + 50, len(pattern_texts))):
                text1 = (
                    pattern_texts[i].split(":", 1)[-1]
                    if ":" in pattern_texts[i]
                    else pattern_texts[i]
                )
                text2 = (
                    pattern_texts[j].split(":", 1)[-1]
                    if ":" in pattern_texts[j]
                    else pattern_texts[j]
                )
                lcs = self._longest_common_substring(text1, text2)
                if len(lcs) >= min_length:
                    common_substrings[lcs] += 1

        if common_substrings:
            table = Table(
                title="Common Error Patterns (Longest Common Substrings)",
                show_header=True,
            )
            table.add_column("Pattern", style="yellow", max_width=60)
            table.add_column("Occurrences", style="cyan", justify="right")

            for substring, count in common_substrings.most_common(10):
                if count > 1:
                    table.add_row(substring, str(count))

            if table.rows:
                console.print(table)
                console.print()

    def _longest_common_substring(self, s1: str, s2: str) -> str:
        m = len(s1)
        n = len(s2)
        dp = [[0] * (n + 1) for _ in range(m + 1)]
        max_length = 0
        ending_pos = 0

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if s1[i - 1] == s2[j - 1]:
                    dp[i][j] = dp[i - 1][j - 1] + 1
                    if dp[i][j] > max_length:
                        max_length = dp[i][j]
                        ending_pos = i

        return s1[ending_pos - max_length : ending_pos]

    def delete_duplicate_messages(self, queue_url: str, max_messages: int) -> None:
        """
        Deletes duplicate messages from an SQS queue based on the 'id' message attribute.

        Logic:
        - Keeps the first message with a given 'id' attribute
        - If a message has the same 'id' but DIFFERENT messageId: DELETE (duplicate resource)
        - If a message has the same 'id' AND same messageId: KEEP (SQS redelivery, valid)

        Args:
            queue_url (str): The URL of the SQS queue.
            max_messages (int): The maximum number of messages to process.
        """
        id_to_message_id = {}
        processed_batches = 0
        batch_size = 10
        max_batches = max_messages // batch_size
        counts = defaultdict(int)

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TextColumn("[green]Kept: {task.fields[kept]}"),
            TextColumn("[yellow]Skipped: {task.fields[skipped]}"),
            TextColumn("[cyan]Deleted: {task.fields[deleted]}"),
            console=console,
        ) as progress:
            task = progress.add_task(
                "Processing messages", total=max_batches, deleted=0, kept=0, skipped=0
            )

            while processed_batches < max_batches:
                response = self.sqs_client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=batch_size,
                    WaitTimeSeconds=1,
                    MessageAttributeNames=["All"],
                    VisibilityTimeout=300,
                )

                messages = response.get("Messages", [])
                if not messages:
                    break

                for message in messages:
                    self._process_message(message, id_to_message_id, counts, queue_url)

                self._update_progress(progress, task, counts)
                processed_batches += 1

        self._print_duplicates_summary(counts)

    def _process_message(
        self,
        message: dict,
        id_to_message_id: dict,
        counts: Dict[str, int],
        queue_url: str,
    ) -> None:
        """Processes a single message for duplicate detection and deletion."""
        message_id = message["MessageId"]
        receipt_handle = message["ReceiptHandle"]
        message_attributes = message.get("MessageAttributes", {})

        identifier_attr = message_attributes.get("id")
        identifier = identifier_attr.get("StringValue")
        if not identifier:
            console.print("Skipping message with missing 'id' attribute.")
            counts["skipped"] += 1
            return

        # Check if we've seen this id before
        if identifier in id_to_message_id:
            stored_message_id = id_to_message_id[identifier]

            if message_id == stored_message_id:
                # Same id, same messageId -> Keep (valid redelivery)
                counts["kept_redelivery"] += 1
            else:
                # Same id, different messageId -> Delete (duplicate resource)
                console.print(f"Deleting duplicate message for {identifier=}")
                counts["deleted"] += 1
                self.delete_message(queue_url, receipt_handle)
        else:
            # First time seeing this id
            counts["kept_first"] += 1
            id_to_message_id[identifier] = message_id

    def _update_progress(
        self, progress: Progress, task: TaskID, counts: Dict[str, int]
    ) -> None:
        """Updates the progress bar with current counts."""
        progress.update(
            task,
            advance=1,
            deleted=counts["deleted"],
            kept=counts["kept_first"] + counts["kept_redelivery"],
            skipped=counts["skipped"],
        )

    def _print_duplicates_summary(self, counts: Dict[str, int]) -> None:
        """Prints a summary table of the processing results."""
        console.print("\n[bold green]Duplicate removal complete![/bold green]\n")

        table = Table(title="Summary")
        table.add_column("Category", style="cyan", no_wrap=True)
        table.add_column("Count", justify="right", style="magenta")

        table.add_row("Kept (first occurrence)", str(counts["kept_first"]))
        table.add_row("Kept (valid redelivery)", str(counts["kept_redelivery"]))
        table.add_row("Deleted (duplicates)", str(counts["deleted"]))
        table.add_row("Skipped (no id)", str(counts["skipped"]))
        table.add_row("Total processed", str(sum(counts.values())), style="bold")

        console.print(table)
