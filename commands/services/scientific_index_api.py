import boto3
import io
import json
import logging
import requests
from datetime import datetime, timedelta

import polars as pl
from tqdm import tqdm

from commands.services.customers_api import get_all_customers

logger = logging.getLogger(__name__)

XLSX_ACCEPT = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
INSTITUTION_REPORT_PATH = "scientific-index/institution-approval-report"


class ScientificIndexService:
    def __init__(self, profile: str):
        self.session = boto3.Session(profile_name=profile)
        self.ssm = self.session.client("ssm")
        self.secretsmanager = self.session.client("secretsmanager")
        params = self._get_system_parameters(["/NVA/ApiDomain", "/NVA/CognitoUri"])
        self.api_domain = params["/NVA/ApiDomain"]
        self.cognito_uri = params["/NVA/CognitoUri"]
        self.client_credentials = self._get_secret("BackendCognitoClientCredentials")
        self.token = self._get_cognito_token()

    def _get_system_parameters(self, names: list[str]) -> dict[str, str]:
        response = self.ssm.get_parameters(Names=names)
        return {param["Name"]: param["Value"] for param in response["Parameters"]}

    def _get_secret(self, name: str) -> dict:
        response = self.secretsmanager.get_secret_value(SecretId=name)
        return json.loads(response["SecretString"])

    def _get_cognito_token(self) -> str:
        url = f"{self.cognito_uri}/oauth2/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_credentials["backendClientId"],
            "client_secret": self.client_credentials["backendClientSecret"],
        }
        response = requests.post(url, headers=headers, data=data)
        response_json = response.json()
        self.token_expiry_time = datetime.now() + timedelta(seconds=response_json["expires_in"])
        return response_json["access_token"]

    def _is_token_expired(self) -> bool:
        return datetime.now() > self.token_expiry_time - timedelta(seconds=30)

    def _get_token(self) -> str:
        if self._is_token_expired():
            self.token = self._get_cognito_token()
        return self.token

    def get_institution_report(self, cristin_id: str, year: int) -> bytes:
        url = f"https://{self.api_domain}/{INSTITUTION_REPORT_PATH}/{year}"
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": XLSX_ACCEPT,
        }
        response = requests.get(url, headers=headers, params={"institutionId": cristin_id})
        response.raise_for_status()
        return response.content

    def get_all_institution_reports(self, profile: str, year: int) -> pl.DataFrame:
        nvi_customers = [
            customer
            for customer in get_all_customers(profile)
            if customer.nvi_institution and customer.cristin_id
        ]

        if not nvi_customers:
            raise ValueError("No NVI institutions found")

        logger.info("Found %d NVI institutions. Fetching reports for %d...", len(nvi_customers), year)
        logging.getLogger("fastexcel").setLevel(logging.ERROR)

        frames: list[pl.DataFrame] = []
        errors: list[str] = []

        for customer in tqdm(nvi_customers, desc="Fetching reports"):
            cristin_short_id = customer.cristin_id.rsplit("/", 1)[-1]
            try:
                data = self.get_institution_report(cristin_short_id, year)
                df = pl.read_excel(io.BytesIO(data), raise_if_empty=False)
                if len(df) > 0:
                    frames.append(df)
            except Exception as error:
                errors.append(f"{customer.name} ({cristin_short_id}): {error}")

        if errors:
            logger.warning("Failed to fetch %d reports:\n%s", len(errors), "\n".join(f"  {e}" for e in errors))

        if not frames:
            raise ValueError("No reports fetched successfully")

        return pl.concat(frames, how="diagonal")
