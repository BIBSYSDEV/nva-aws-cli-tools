import boto3
import json
import logging
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

XLSX_ACCEPT = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
INSTITUTION_REPORT_PATH = "scientific-index/institution-approval-report"


class ScientificIndexService:
    def __init__(self, profile: str):
        self.session = boto3.Session(profile_name=profile)
        self.ssm = self.session.client("ssm")
        self.secretsmanager = self.session.client("secretsmanager")
        self.api_domain = self._get_system_parameter("/NVA/ApiDomain")
        self.cognito_uri = self._get_system_parameter("/NVA/CognitoUri")
        self.client_credentials = self._get_secret("BackendCognitoClientCredentials")
        self.token = self._get_cognito_token()
        self.token_expiry_time = datetime.now()

    def _get_system_parameter(self, name: str) -> str:
        response = self.ssm.get_parameter(Name=name)
        return response["Parameter"]["Value"]

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
