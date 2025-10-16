import json
import random
import signal
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

console = Console()


class SqsService:
    def __init__(self, profile: Optional[str] = None):
        if profile:
            self.session = boto3.Session(profile_name=profile)
        else:
            self.session = boto3.Session()
        self.sqs_client = self.session.client('sqs')
        self.profile = profile or 'default'

    def find_queue_url(self, queue_name_partial: str) -> Optional[str]:
        try:
            response = self.sqs_client.list_queues()
            queue_urls = response.get('QueueUrls', [])

            matching_queues = []
            for url in queue_urls:
                queue_name = url.split('/')[-1]
                if queue_name_partial.lower() in queue_name.lower():
                    matching_queues.append(url)

            if not matching_queues:
                console.print(f"[red]No queues found matching '{queue_name_partial}'[/red]")
                return None

            if len(matching_queues) == 1:
                queue_name = matching_queues[0].split('/')[-1]
                console.print(f"[green]Found queue: {queue_name}[/green]")
                return matching_queues[0]

            table = Table(title="Multiple queues found")
            table.add_column("Index", style="cyan")
            table.add_column("Queue Name", style="yellow")

            for i, url in enumerate(matching_queues):
                queue_name = url.split('/')[-1]
                table.add_row(str(i + 1), queue_name)

            console.print(table)

            while True:
                choice = console.input("[cyan]Select queue by index (or 'q' to quit): [/cyan]")
                if choice.lower() == 'q':
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
            return None

    def get_queue_attributes(self, queue_url: str) -> Optional[Dict[str, Any]]:
        try:
            response = self.sqs_client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['All']
            )
            return response.get('Attributes', {})
        except ClientError as e:
            console.print(f"[red]Error getting queue attributes: {e}[/red]")
            return None

    def receive_messages(self, queue_url: str, max_messages: int = 10) -> List[Dict[str, Any]]:
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=min(max_messages, 10),
                MessageAttributeNames=['All'],
                AttributeNames=['All'],
                WaitTimeSeconds=20
            )

            messages = response.get('Messages', [])

            processed_messages = []
            for msg in messages:
                processed_msg = {
                    'MessageId': msg.get('MessageId'),
                    'ReceiptHandle': msg.get('ReceiptHandle'),
                    'Body': msg.get('Body'),
                    'Attributes': msg.get('Attributes', {}),
                    'MessageAttributes': msg.get('MessageAttributes', {}),
                    'MD5OfBody': msg.get('MD5OfBody'),
                    'MD5OfMessageAttributes': msg.get('MD5OfMessageAttributes')
                }

                try:
                    processed_msg['ParsedBody'] = json.loads(msg.get('Body', '{}'))
                except (json.JSONDecodeError, TypeError):
                    processed_msg['ParsedBody'] = None

                processed_messages.append(processed_msg)

            return processed_messages

        except ClientError as e:
            console.print(f"[red]Error receiving messages: {e}[/red]")
            return []

    def delete_message(self, queue_url: str, receipt_handle: str) -> bool:
        try:
            self.sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            return True
        except ClientError as e:
            console.print(f"[red]Error deleting message: {e}[/red]")
            return False

    def delete_message_batch(self, queue_url: str, receipt_handles: List[str]) -> int:
        if not receipt_handles:
            return 0

        deleted_count = 0
        for i in range(0, len(receipt_handles), 10):
            batch = receipt_handles[i:i+10]
            entries = [
                {'Id': str(j), 'ReceiptHandle': handle}
                for j, handle in enumerate(batch)
            ]

            try:
                response = self.sqs_client.delete_message_batch(
                    QueueUrl=queue_url,
                    Entries=entries
                )
                deleted_count += len(response.get('Successful', []))

                failed = response.get('Failed', [])
                if failed:
                    for failure in failed:
                        console.print(f"[yellow]Failed to delete message: {failure.get('Message')}[/yellow]")

            except ClientError as e:
                console.print(f"[red]Error deleting message batch: {e}[/red]")

        return deleted_count

    def _drain_worker_thread(self, queue_url: str, base_dir: Path,
                            max_messages_per_file: int, delete_after_write: bool,
                            stop_event: threading.Event, stats: Dict[str, Any]) -> None:
        """Worker thread that receives, writes, and deletes messages."""
        sqs_client = self.session.client('sqs')

        messages_buffer = []
        receipt_handles_buffer = []
        consecutive_empty = 0

        while not stop_event.is_set() and consecutive_empty < 3:
            try:
                response = sqs_client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    MessageAttributeNames=['All'],
                    AttributeNames=['All'],
                    WaitTimeSeconds=0
                )

                messages = response.get('Messages', [])

                if not messages:
                    consecutive_empty += 1
                    if consecutive_empty >= 3 and messages_buffer:
                        with stats['lock']:
                            stats['file_counter'] += 1
                            file_num = stats['file_counter']

                        output_file = base_dir / f"messages_{file_num:04d}.jsonl"
                        content = ''.join(
                            json.dumps({k: v for k, v in msg.items() if k != 'ReceiptHandle'}, default=str) + '\n'
                            for msg in messages_buffer
                        )
                        output_file.write_text(content)

                        with stats['lock']:
                            stats['written'] += len(messages_buffer)

                        if delete_after_write and receipt_handles_buffer:
                            deleted = self.delete_message_batch(queue_url, receipt_handles_buffer)
                            with stats['lock']:
                                stats['deleted'] += deleted

                        messages_buffer = []
                        receipt_handles_buffer = []
                    continue

                consecutive_empty = 0

                for msg in messages:
                    processed_msg = {
                        'MessageId': msg.get('MessageId'),
                        'ReceiptHandle': msg.get('ReceiptHandle'),
                        'Body': msg.get('Body'),
                        'Attributes': msg.get('Attributes', {}),
                        'MessageAttributes': msg.get('MessageAttributes', {}),
                        'MD5OfBody': msg.get('MD5OfBody'),
                        'MD5OfMessageAttributes': msg.get('MD5OfMessageAttributes')
                    }

                    try:
                        processed_msg['ParsedBody'] = json.loads(msg.get('Body', '{}'))
                    except (json.JSONDecodeError, TypeError):
                        processed_msg['ParsedBody'] = None

                    messages_buffer.append(processed_msg)
                    receipt_handles_buffer.append(msg['ReceiptHandle'])

                    with stats['lock']:
                        stats['received'] += 1

                    if len(messages_buffer) >= max_messages_per_file:
                        with stats['lock']:
                            stats['file_counter'] += 1
                            file_num = stats['file_counter']

                        output_file = base_dir / f"messages_{file_num:04d}.jsonl"
                        content = ''.join(
                            json.dumps({k: v for k, v in msg.items() if k != 'ReceiptHandle'}, default=str) + '\n'
                            for msg in messages_buffer
                        )
                        output_file.write_text(content)

                        with stats['lock']:
                            stats['written'] += len(messages_buffer)

                        if delete_after_write:
                            deleted = self.delete_message_batch(queue_url, receipt_handles_buffer)
                            with stats['lock']:
                                stats['deleted'] += deleted

                        messages_buffer = []
                        receipt_handles_buffer = []

            except Exception:
                if stop_event.is_set():
                    break

    def drain_queue(self, queue_name_partial: str, output_dir: Optional[str] = None,
                   max_messages_per_file: int = 1000, delete_after_write: bool = True,
                   num_threads: int = 5) -> bool:
        """Drain all messages from a queue using multiple threads."""
        queue_url = self.find_queue_url(queue_name_partial)
        if not queue_url:
            return False

        queue_name = queue_url.split('/')[-1]

        if num_threads == 1:
            console.print("[cyan]Using single-threaded mode[/cyan]")
            return self._drain_single_thread(
                queue_url, queue_name, output_dir, max_messages_per_file, delete_after_write
            )

        console.print(f"[cyan]Using {num_threads} parallel threads[/cyan]")

        queue_attrs = self.get_queue_attributes(queue_url)
        if queue_attrs:
            approx_messages = int(queue_attrs.get('ApproximateNumberOfMessages', 0))
            console.print(f"[cyan]Queue has approximately {approx_messages} messages[/cyan]")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if output_dir:
            base_dir = Path(output_dir)
        else:
            base_dir = Path(f"{self.profile}-{queue_name}-{timestamp}")

        base_dir.mkdir(parents=True, exist_ok=True)
        console.print(f"[green]Output directory: {base_dir}[/green]")

        metadata_file = base_dir / "metadata.json"
        metadata_file.write_text(json.dumps({
            'queue_url': queue_url,
            'queue_name': queue_name,
            'profile': self.profile,
            'timestamp': timestamp,
            'attributes': queue_attrs or {},
            'num_threads': num_threads
        }, indent=2, default=str))

        stats = {
            'received': 0,
            'written': 0,
            'deleted': 0,
            'file_counter': 0,
            'lock': threading.Lock()
        }

        stop_event = threading.Event()

        def signal_handler(*args):
            console.print("\n[yellow]Stopping... Please wait for threads to finish current batch[/yellow]")
            stop_event.set()

        old_handler = signal.signal(signal.SIGINT, signal_handler)

        try:
            threads = []
            for _ in range(num_threads):
                thread = threading.Thread(
                    target=self._drain_worker_thread,
                    args=(queue_url, base_dir, max_messages_per_file,
                          delete_after_write, stop_event, stats),
                    daemon=False
                )
                thread.start()
                threads.append(thread)

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task(
                    f"[cyan]Draining queue {queue_name}...",
                    total=None
                )

                last_written = 0
                while any(t.is_alive() for t in threads):
                    current_written = stats['written']
                    if current_written != last_written or stats['received'] % 100 == 0:
                        progress.update(
                            task,
                            description=f"[cyan]Draining... (received: {stats['received']}, "
                                       f"written: {stats['written']}, files: {stats['file_counter']})"
                        )
                        last_written = current_written

                    threading.Event().wait(0.1)

                    if stop_event.is_set():
                        break

            for thread in threads:
                thread.join(timeout=5)

        finally:
            signal.signal(signal.SIGINT, old_handler)

        summary_file = base_dir / "summary.json"
        summary_file.write_text(json.dumps({
            'queue_name': queue_name,
            'profile': self.profile,
            'total_messages': stats['written'],
            'files_created': stats['file_counter'],
            'messages_deleted': delete_after_write,
            'messages_deleted_count': stats['deleted'],
            'num_threads': num_threads,
            'timestamp_start': timestamp,
            'timestamp_end': datetime.now().strftime("%Y%m%d_%H%M%S")
        }, indent=2))

        console.print(f"\n[bold green]✓ Drained {stats['written']} messages from {queue_name}[/bold green]")
        console.print(f"[cyan]Output: {base_dir}[/cyan]")
        console.print(f"[cyan]Files created: {stats['file_counter']}[/cyan]")
        if delete_after_write:
            console.print(f"[cyan]Messages deleted: {stats['deleted']}[/cyan]")

        return True

    def _drain_single_thread(self, queue_url: str, queue_name: str,
                            output_dir: Optional[str], max_messages_per_file: int,
                            delete_after_write: bool) -> bool:
        """Simple single-threaded drain implementation."""
        queue_attrs = self.get_queue_attributes(queue_url)
        if queue_attrs:
            approx_messages = int(queue_attrs.get('ApproximateNumberOfMessages', 0))
            console.print(f"[cyan]Queue has approximately {approx_messages} messages[/cyan]")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if output_dir:
            base_dir = Path(output_dir)
        else:
            base_dir = Path(f"{self.profile}-{queue_name}-{timestamp}")

        base_dir.mkdir(parents=True, exist_ok=True)
        console.print(f"[green]Output directory: {base_dir}[/green]")

        metadata_file = base_dir / "metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump({
                'queue_url': queue_url,
                'queue_name': queue_name,
                'profile': self.profile,
                'timestamp': timestamp,
                'attributes': queue_attrs or {}
            }, f, indent=2, default=str)

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
                f"[cyan]Draining queue {queue_name}...",
                total=None
            )

            consecutive_empty_receives = 0
            max_empty_receives = 3

            while consecutive_empty_receives < max_empty_receives:
                messages = self.receive_messages(queue_url, max_messages=10)

                if not messages:
                    consecutive_empty_receives += 1
                    continue

                consecutive_empty_receives = 0

                for msg in messages:
                    messages_buffer.append(msg)
                    receipt_handles_buffer.append(msg['ReceiptHandle'])
                    total_messages += 1

                    if len(messages_buffer) >= max_messages_per_file:
                        file_count += 1
                        output_file = base_dir / f"messages_{file_count:04d}.jsonl"

                        with open(output_file, 'w') as f:
                            for buffered_msg in messages_buffer:
                                saved_msg = {k: v for k, v in buffered_msg.items()
                                           if k != 'ReceiptHandle'}
                                f.write(json.dumps(saved_msg, default=str) + '\n')

                        console.print(f"[green]Wrote {len(messages_buffer)} messages to {output_file.name}[/green]")

                        if delete_after_write:
                            deleted = self.delete_message_batch(queue_url, receipt_handles_buffer)
                            if deleted != len(receipt_handles_buffer):
                                console.print(f"[yellow]Warning: Only deleted {deleted}/{len(receipt_handles_buffer)} messages[/yellow]")

                        messages_buffer = []
                        receipt_handles_buffer = []

                progress.update(task, description=f"[cyan]Draining queue {queue_name}... ({total_messages} messages processed)")

            if messages_buffer:
                file_count += 1
                output_file = base_dir / f"messages_{file_count:04d}.jsonl"

                with open(output_file, 'w') as f:
                    for buffered_msg in messages_buffer:
                        saved_msg = {k: v for k, v in buffered_msg.items()
                                   if k != 'ReceiptHandle'}
                        f.write(json.dumps(saved_msg, default=str) + '\n')

                console.print(f"[green]Wrote {len(messages_buffer)} messages to {output_file.name}[/green]")

                if delete_after_write:
                    deleted = self.delete_message_batch(queue_url, receipt_handles_buffer)
                    if deleted != len(receipt_handles_buffer):
                        console.print(f"[yellow]Warning: Only deleted {deleted}/{len(receipt_handles_buffer)} messages[/yellow]")

        summary_file = base_dir / "summary.json"
        with open(summary_file, 'w') as f:
            json.dump({
                'queue_name': queue_name,
                'profile': self.profile,
                'total_messages': total_messages,
                'files_created': file_count,
                'messages_deleted': delete_after_write,
                'timestamp_start': timestamp,
                'timestamp_end': datetime.now().strftime("%Y%m%d_%H%M%S")
            }, f, indent=2)

        console.print(f"\n[bold green]✓ Drained {total_messages} messages from {queue_name}[/bold green]")
        console.print(f"[cyan]Output: {base_dir}[/cyan]")
        console.print(f"[cyan]Files created: {file_count}[/cyan]")

        return True