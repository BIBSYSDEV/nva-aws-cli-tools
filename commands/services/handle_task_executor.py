from .handle_api import HandleApiService
from .publication_api import PublicationApiService
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
        self.publication_service = PublicationApiService(profile)
        self.default_action = lambda task: None
        self.switcher = {
            "nop": lambda task: None,
            "move_top_to_additional_and_promote_additional": lambda task: self.move_top_to_additional_and_promote_additional(
                task
            ),
            "move_top_to_additional_and_create_new_top": lambda task: self.move_top_to_additional_and_create_new_top(
                task
            ),
            "promote_additional": lambda task: self.promote_additional(task),
            "create_new_top": lambda task: self.create_new_top(task),
        }
        pass

    def __del__(self):
        if self.sqlite_conn:
            self.sqlite_conn.close()

    def move_top_to_additional_and_promote_additional(self, task):
        # Logic to move top handle to additional and promote additional
        print("move_top_to_additional_and_promote_additional: " + task.get("action"))
        pass

    def move_top_to_additional_and_create_new_top(self, task):
        # Logic to move top handle to additional and create new top handle
        print("move_top_to_additional_and_create_new_top: " + task.get("action"))
        pass

    def promote_additional(self, task):
        # Logic to promote additional handle
        print("promote_additional: " + task.get("action"))
        pass

    def create_new_top(self, task):
        # Logic to create new top handle
        print("create_new_top: " + task.get("action"))
        pass

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

    def import_handles(self, item):
        handles_to_import = item.get("handles_to_import", [])
        for handle in handles_to_import:
            path_segments = handle.split("/")
            suffix = path_segments.pop()  # Last segment
            prefix = path_segments.pop()  # Second last segment
            value = self.publication_service.get_uri(item.get("identifier"))
            print(f"Create handle {prefix}/{suffix} with value " + value)
            request_body = {
                "uri": value,
                "prefix": prefix,
                "suffix": suffix,
            }
            result = self.handle_service.create_handle(request_body)
            print(f"Handle {prefix}/{suffix} was created with result: {result}")
        pass

    def execute(self, batch):
        for item in batch:
            task_id = item.get("identifier") + item.get("action")
            if self.is_task_done(task_id):
                print(f"Task {task_id} already done, skipping.")
                continue
            action = item.get("action")

            func = self.switcher.get(action, self.default_action)
            if func is not self.default_action:
                # func(item) # TODO: activate this when the functions are implemented
                self.import_handles(item)
                self.log_task_done(task_id)
            else:
                print(f"Unknown action: {action}")
                exit(1)
        pass
