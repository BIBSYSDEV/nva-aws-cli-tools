from dataclasses import dataclass


@dataclass
class AppContext:
    log_level: int
    profile: str
