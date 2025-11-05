from .handle_api import HandleApiService
import sqlite3
from datetime import datetime
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading


class HandleTaskExecutorService:
    def __init__(self, profile, folder, max_workers=5):
        self.folder = folder
        self.max_workers = max_workers
        self.db_path = os.path.join(folder, "done_tasks.db")

        # Create database schema in main thread
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS done_tasks (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                success INTEGER NOT NULL,
                output TEXT NOT NULL
            )
        """)
        conn.commit()
        conn.close()

        # Connection for reading in main thread
        self.main_conn = sqlite3.connect(self.db_path)
        self.main_cursor = self.main_conn.cursor()

        self.handle_service = HandleApiService(profile)
        self.db_write_queue = Queue()
        self.db_thread_running = False
        self.db_thread = None

    def __del__(self):
        self._stop_db_writer()
        if self.main_conn:
            self.main_conn.close()

    def _start_db_writer(self):
        if not self.db_thread_running:
            self.db_thread_running = True
            self.db_thread = threading.Thread(target=self._db_writer_worker, daemon=True)
            self.db_thread.start()

    def _stop_db_writer(self):
        if self.db_thread_running:
            self.db_write_queue.put(None)
            if self.db_thread:
                self.db_thread.join()
            self.db_thread_running = False

    def _db_writer_worker(self):
        # Create a new SQLite connection in this thread
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            while True:
                item = self.db_write_queue.get()
                if item is None:
                    break
                task_id, success, output = item
                self._write_task_to_db(cursor, conn, task_id, success, output)
                self.db_write_queue.task_done()
        finally:
            conn.close()

    def _write_task_to_db(self, cursor, conn, task_id, success, output):
        now = datetime.now().isoformat()
        cursor.execute(
            """
            INSERT OR REPLACE INTO done_tasks (id, timestamp, success, output)
            VALUES (?, ?, ?, ?)
        """,
            (task_id, now, 1 if success else 0, output),
        )
        conn.commit()

    def log_task_done(self, task_id, success, output):
        self.db_write_queue.put((task_id, success, output))

    def is_task_done(self, task_id):
        self.main_cursor.execute(
            """
            SELECT id FROM done_tasks WHERE id = ?
        """,
            (task_id,),
        )
        task = self.main_cursor.fetchone()
        return task is not None

    def create_handle(self, task):
        handle_url = task.get("handle")
        publication_uri = task.get("publication_uri")

        request_body = {
            "uri": publication_uri
        }

        if handle_url:
            path_segments = handle_url.replace("https://hdl.handle.net/", "").split("/")
            prefix = path_segments[0]
            suffix = "/".join(path_segments[1:])
            request_body["prefix"] = prefix
            request_body["suffix"] = suffix
            print(f"Creating handle {prefix}/{suffix} with value {publication_uri}")
        else:
            print(f"Creating handle with value {publication_uri}")

        result = self.handle_service.create_handle(request_body)

        if isinstance(result, dict) and "status" in result:
            success = 200 <= result.get("status") < 300
        else:
            success = True

        output = json.dumps(result)

        print(f"Handle created with result: {result}")
        return success, output

    def update_handle(self, task):
        handle_url = task.get("handle")
        publication_uri = task.get("publication_uri")

        path_segments = handle_url.replace("https://hdl.handle.net/", "").split("/")
        prefix = path_segments[0]
        suffix = "/".join(path_segments[1:])

        print(f"Updating handle {prefix}/{suffix} with value {publication_uri}")
        request_body = {
            "uri": publication_uri
        }
        result = self.handle_service.update_handle(prefix, suffix, request_body)

        if isinstance(result, dict) and "status" in result:
            success = 200 <= result.get("status") < 300
        else:
            success = True

        output = json.dumps(result)

        print(f"Handle {prefix}/{suffix} updated with result: {result}")
        return success, output

    def _process_create_task(self, task, task_id):
        success, output = self.create_handle(task)
        self.log_task_done(task_id, success, output)

    def _process_update_task(self, task, task_id):
        success, output = self.update_handle(task)
        self.log_task_done(task_id, success, output)

    def execute_create(self, batch):
        self._start_db_writer()

        tasks_to_process = []
        for task in batch:
            identifier = task.get("identifier")
            handle = task.get("handle", "auto")
            task_id = f"{identifier}:{handle}:create"

            if self.is_task_done(task_id):
                print(f"Task {task_id} already done, skipping.")
                continue

            tasks_to_process.append((task, task_id))

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._process_create_task, task, task_id)
                      for task, task_id in tasks_to_process]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing task: {e}")

        self.db_write_queue.join()

    def execute_update(self, batch):
        self._start_db_writer()

        tasks_to_process = []
        for task in batch:
            identifier = task.get("identifier")
            handle = task.get("handle")
            task_id = f"{identifier}:{handle}:update"

            if self.is_task_done(task_id):
                print(f"Task {task_id} already done, skipping.")
                continue

            tasks_to_process.append((task, task_id))

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._process_update_task, task, task_id)
                      for task, task_id in tasks_to_process]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing task: {e}")

        self.db_write_queue.join()
