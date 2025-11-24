import boto3
import logging
import requests
import json
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

"""
# example of usage


"""


class PublicationApiService:
    def __init__(self, profile, client_id=None, client_secret=None):
        self.session = boto3.Session(profile_name=profile)
        self.ssm = self.session.client("ssm")
        self.secretsmanager = self.session.client("secretsmanager")
        self.api_domain = self._get_system_parameter("/NVA/ApiDomain")
        self.cognito_uri = self._get_system_parameter("/NVA/CognitoUri")
        if client_id and client_secret:
            self.client_credentials = {
                "backendClientId": client_id,
                "backendClientSecret": client_secret,
            }
        else:
            self.client_credentials = self._get_secret(
                "BackendCognitoClientCredentials"
            )
        self.token = self._get_cognito_token()
        self.token_expiry_time = datetime.now()  # Initialize with current time

    def _get_system_parameter(self, name):
        response = self.ssm.get_parameter(Name=name)
        return response["Parameter"]["Value"]

    def _get_secret(self, name):
        response = self.secretsmanager.get_secret_value(SecretId=name)
        secret_string = response["SecretString"]
        secret = json.loads(secret_string)
        return secret

    def _get_cognito_token(self):
        url = f"{self.cognito_uri}/oauth2/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_credentials["backendClientId"],
            "client_secret": self.client_credentials["backendClientSecret"],
        }
        response = requests.post(url, headers=headers, data=data)
        response_json = response.json()
        self.token_expiry_time = datetime.now() + timedelta(
            seconds=response_json["expires_in"]
        )  # Set the expiry time
        return response_json["access_token"]

    def _is_token_expired(self):
        # If there are less than 30 seconds until the token expires, consider it expired
        return datetime.now() > self.token_expiry_time - timedelta(seconds=30)

    def _get_token(self):
        if self._is_token_expired():
            self.token = self._get_cognito_token()
        return self.token

    def fetch_publication(self, publicationIdentifier, doNotRedirect=True):
        url = f"{self.get_uri(publicationIdentifier)}?doNotRedirect={doNotRedirect}"
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:  # If the status code indicates success
            return response.json()
        else:
            logger.error(
                "Failed to fetch publication. Status code:", response.status_code
            )
            return None

    def update_publication(self, publicationIdentifier, request_body):
        url = self.get_uri(publicationIdentifier)
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        response = requests.put(url, headers=headers, json=request_body)
        return response.json()

    def create_publication(self, request_body):
        url = f"https://{self.api_domain}/publication"
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        response = requests.post(url, headers=headers, json=request_body)
        return response.json()

    def get_uri(self, publicationIdentifier):
        return f"https://{self.api_domain}/publication/{publicationIdentifier}"
