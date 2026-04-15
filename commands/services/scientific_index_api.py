import boto3
import json
import logging
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

XLSX_AUTHOR_SHARES_ACCEPT = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; profile=https://api.nva.unit.no/report/author-shares"
ALL_INSTITUTIONS_REPORT_PATH = "scientific-index/reports/{year}/institutions"


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

    def get_all_institutions_report(self, year: int) -> bytes:
        url = f"https://{self.api_domain}/{ALL_INSTITUTIONS_REPORT_PATH.format(year=year)}"
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": XLSX_AUTHOR_SHARES_ACCEPT,
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.content
