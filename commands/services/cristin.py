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
        credentials = self._get_secret("CristinClientBasicAuth")
        self.auth = base64.b64encode(
            f"{credentials['username']}:{credentials['password']}".encode("utf-8")
        ).decode("utf-8")

    def add_person(self, person):
        http_client = requests.Session()

        response = http_client.post(
            f"{self.cristin_api}/persons",
            json=person,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Basic {self.auth}",
                self.bypass_header: self.bypass_value,
                "Cristin-Representing-Institution": "20754",
            },
        )
        if not response.ok:
            print(response.text)
            return response.text

        return response.json()

    def update_person(self, user_id, person):
        http_client = requests.Session()

        person.pop("cristin_person_id", None)
        person.pop("norwegian_national_id", None)
        print(person)

        # Perform the PATCH request
        response = http_client.patch(
            f"{self.cristin_api}/persons/{user_id}",
            json=person,
            headers={
                "Content-Type": "application/merge-patch+json",
                "Accept": "application/json",
                "Authorization": f"Basic {self.auth}",
                self.bypass_header: self.bypass_value,
                "Cristin-Representing-Institution": "20754",
            },
        )

        # Handle HTTP response
        if not response.ok:
            print(f"Error while updating person (ID: {user_id}): {response.text}")
            return response.text

        print(f"Person (ID: {user_id}) updated successfully.")

    def _get_system_parameter(self, name):
        response = self.ssm.get_parameter(Name=name)
        return response["Parameter"]["Value"]

    def _get_secret(self, name):
        response = self.secretsmanager.get_secret_value(SecretId=name)
        secret_string = response["SecretString"]
        secret = json.loads(secret_string)
        return secret
