import boto3
import requests
import json
import base64


class CristinService:
    def __init__(self, profile):
        self.profile = profile
        session = (
            boto3.Session(profile_name=self.profile)
            if self.profile
            else boto3.Session()
        )
        self.ssm = session.client("ssm")
        self.secretsmanager = session.client("secretsmanager")

        self.cristin_api = f"https://{self._get_system_parameter('cristinRestApi')}"
        self.bypass_header = self._get_system_parameter(
            "CristinBotFilterBypassHeaderName"
        )
        self.bypass_value = self._get_system_parameter(
            "CristinBotFilterBypassHeaderValue"
        )
        self.credentials = self._get_secret("CristinClientBasicAuth")

    def add_person(self, person):
        http_client = requests.Session()
        auth = base64.b64encode(
            f"{self.credentials['username']}:{self.credentials['password']}".encode(
            "utf-8"
            )
        ).decode("utf-8")

        response = http_client.post(
            f"{self.cristin_api}/persons",
            json=person,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Basic {auth}",
                self.bypass_header: self.bypass_value,
            },
        )
        if not response.ok:
            print(response.text)
            return response.text
        
        return response.json()

    def _get_system_parameter(self, name):
        response = self.ssm.get_parameter(Name=name)
        return response["Parameter"]["Value"]

    def _get_secret(self, name):
        response = self.secretsmanager.get_secret_value(SecretId=name)
        secret_string = response["SecretString"]
        secret = json.loads(secret_string)
        return secret
