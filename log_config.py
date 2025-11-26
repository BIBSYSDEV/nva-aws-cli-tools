import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from rich.console import Console
from rich.logging import RichHandler
from pythonjsonlogger.json import JsonFormatter

log_console = Console(stderr=True)


def get_log_level(verbose: bool):
    if verbose:
        return logging.DEBUG
    return logging.INFO


def get_json_handler(log_file: Path = Path("logs.jsonl")):
    json_formatter = JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(message)s %(name)s",
        rename_fields={"asctime": "time", "levelname": "level"},
    )
    json_handler = RotatingFileHandler(
        filename=log_file,
        mode="a",
        maxBytes=2097152,  # 2 MB
        backupCount=5,
        encoding="utf8",
    )
    json_handler.setFormatter(json_formatter)
    return json_handler


def get_rich_handler(log_level=logging.INFO):
    console_handler = RichHandler(
        console=log_console,
        level=log_level,
        rich_tracebacks=True,
        log_time_format="%H:%M:%S",
    )
    console_handler.setFormatter(
        logging.Formatter("%(message)s (%(name)s.%(funcName)s)")
    )
    return console_handler


def configure_logger(verbose=False) -> None:
    log_level = get_log_level(verbose)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(get_rich_handler(log_level))
    logger.addHandler(get_json_handler())
