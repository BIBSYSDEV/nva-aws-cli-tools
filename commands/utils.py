from dataclasses import dataclass

import boto3


@dataclass
class AppContext:
    log_level: int
    profile: str | None
    session: boto3.Session
