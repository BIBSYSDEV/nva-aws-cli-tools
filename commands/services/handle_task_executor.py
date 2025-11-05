from .handle_api import HandleApiService
import sqlite3
from datetime import datetime
import os


class HandleTaskExecutorService:
    def __init__(self, profile, folder):
        self.sqlite_conn = sqlite3.connect(os.path.join(folder, "done_tasks.db"))
        self.sqlite_cursor = self.sqlite_conn.cursor()
        self.sqlite_cursor.execute("""
            CREATE TABLE IF NOT EXISTS done_tasks (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL
            )
        """)
        self.sqlite_conn.commit()
        self.handle_service = HandleApiService(profile)

    def __del__(self):
        if self.sqlite_conn:
            self.sqlite_conn.close()

    def log_task_done(self, task_id):
        now = datetime.now().isoformat()
        self.sqlite_cursor.execute(
            """
            INSERT INTO done_tasks (id, timestamp)
            VALUES (?, ?)
        """,
            (task_id, now),
        )
        self.sqlite_conn.commit()

    def is_task_done(self, task_id):
        self.sqlite_cursor.execute(
            """
            SELECT id FROM done_tasks WHERE id = ?
        """,
            (task_id,),
        )
        task = self.sqlite_cursor.fetchone()
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
        print(f"Handle created with result: {result}")
        return result

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
        print(f"Handle {prefix}/{suffix} updated with result: {result}")
        return result

    def execute_create(self, batch):
        for task in batch:
            identifier = task.get("identifier")
            handle = task.get("handle", "auto")
            task_id = f"{identifier}:{handle}:create"

            if self.is_task_done(task_id):
                print(f"Task {task_id} already done, skipping.")
                continue

            self.create_handle(task)
            self.log_task_done(task_id)

    def execute_update(self, batch):
        for task in batch:
            identifier = task.get("identifier")
            handle = task.get("handle")
            task_id = f"{identifier}:{handle}:update"

            if self.is_task_done(task_id):
                print(f"Task {task_id} already done, skipping.")
                continue

            self.update_handle(task)
            self.log_task_done(task_id)
