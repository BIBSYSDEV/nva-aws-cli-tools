import logging
import time
from datetime import datetime, timedelta

import requests

from commands.services.api_client import ApiClient

logger = logging.getLogger(__name__)

XLSX_AUTHOR_SHARES_ACCEPT = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; profile=https://api.nva.unit.no/report/author-shares"
XLSX_AUTHOR_SHARES_CONTROL_ACCEPT = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; profile=https://api.nva.unit.no/report/author-shares-control"
ALL_PERIODS_REPORT_PATH = "scientific-index/reports"
ALL_INSTITUTIONS_REPORT_PATH = "scientific-index/reports/{year}/institutions"
INSTITUTION_REPORT_PATH = "scientific-index/reports/{year}/institutions/{institution}"
JSON_ACCEPT = "application/json"
POLL_INTERVAL_SECONDS = 5


def fetch_report_json(client: ApiClient, path: str) -> dict:
    url = f"https://{client.api_domain}/{path}"
    headers = {**client.auth_header(), "Accept": JSON_ACCEPT}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_all_institutions_report(
    client: ApiClient,
    year: int,
    timeout_minutes: int = 5,
    institution: str | None = None,
) -> bytes:
    return _fetch_institutions_report(
        client, year, XLSX_AUTHOR_SHARES_ACCEPT, timeout_minutes, institution
    )


def get_all_institutions_report_control(
    client: ApiClient,
    year: int,
    timeout_minutes: int = 5,
    institution: str | None = None,
) -> bytes:
    return _fetch_institutions_report(
        client, year, XLSX_AUTHOR_SHARES_CONTROL_ACCEPT, timeout_minutes, institution
    )


def _fetch_institutions_report(
    client: ApiClient,
    year: int,
    accept: str,
    timeout_minutes: int,
    institution: str | None = None,
) -> bytes:
    path = (
        INSTITUTION_REPORT_PATH.format(year=year, institution=institution)
        if institution
        else ALL_INSTITUTIONS_REPORT_PATH.format(year=year)
    )
    url = f"https://{client.api_domain}/{path}"
    headers = {**client.auth_header(), "Accept": accept}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    presigned_url = response.json()["uri"]
    return _poll_for_xlsx(presigned_url, timeout_minutes)


def _poll_for_xlsx(presigned_url: str, timeout_minutes: int) -> bytes:
    deadline = datetime.now() + timedelta(minutes=timeout_minutes)
    attempt = 0
    while datetime.now() < deadline:
        attempt += 1
        response = requests.get(presigned_url)
        if response.status_code == 200:
            return response.content
        if response.status_code != 404:
            response.raise_for_status()
        logger.debug(
            "Attempt %d: report not ready, retrying in %ds...",
            attempt,
            POLL_INTERVAL_SECONDS,
        )
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(f"Report not available after {timeout_minutes} minutes")
