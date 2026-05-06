import logging
import os
import sqlite3
from datetime import datetime

import boto3

from commands.services.api_client import ApiClient
from commands.services.handle_api import create_handle
from commands.services.publication_api import PublicationApiService

logger = logging.getLogger(__name__)


class HandleTaskExecutorService:
    def __init__(self, session: boto3.Session, profile: str | None, folder: str):
        self.sqlite_conn = sqlite3.connect(os.path.join(folder, "done_tasks.db"))
        self.sqlite_cursor = self.sqlite_conn.cursor()
        self.sqlite_cursor.execute("""
            CREATE TABLE IF NOT EXISTS done_tasks (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL
            )
        """)
        self.sqlite_conn.commit()
        self.api_client = ApiClient(session=session)
        # PublicationApiService is converted in a later PR; bridges via profile for now.
        self.publication_service = PublicationApiService(profile)
        self.default_action = lambda task: None
        self.switcher = {
            "nop": lambda task: None,
            "move_top_to_additional_and_promote_additional": lambda task: (
                self.move_top_to_additional_and_promote_additional(task)
            ),
            "move_top_to_additional_and_create_new_top": lambda task: (
                self.move_top_to_additional_and_create_new_top(task)
            ),
            "promote_additional": lambda task: self.promote_additional(task),
            "create_new_top": lambda task: self.create_new_top(task),
        }

    def __del__(self):
        if self.sqlite_conn:
            self.sqlite_conn.close()

    def move_top_to_additional_and_promote_additional(self, task):
        raise NotImplementedError("Work in progress")

    def move_top_to_additional_and_create_new_top(self, task):
        raise NotImplementedError("Work in progress")

    def promote_additional(self, task):
        raise NotImplementedError("Work in progress")

    def create_new_top(self, task):
        raise NotImplementedError("Work in progress")

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
        for handle in item.get("handles_to_import", []):
            path_segments = handle.split("/")
            suffix = path_segments.pop()
            prefix = path_segments.pop()
            value = self.publication_service.get_uri(item.get("identifier"))
            logger.info(f"Create handle {prefix}/{suffix} with value " + value)
            request_body = {"uri": value, "prefix": prefix, "suffix": suffix}
            result = create_handle(self.api_client, request_body)
            logger.info(f"Handle {prefix}/{suffix} was created with result: {result}")

    def execute(self, batch):
        for item in batch:
            task_id = item.get("identifier") + item.get("action")
            if self.is_task_done(task_id):
                logger.info(f"Task {task_id} already done, skipping.")
                continue
            action = item.get("action")

            func = self.switcher.get(action, self.default_action)
            if func is not self.default_action:
                # func(item) # TODO: activate this when the functions are implemented
                self.import_handles(item)
                self.log_task_done(task_id)
            else:
                logger.error(f"Unknown action: {action}")
                exit(1)
