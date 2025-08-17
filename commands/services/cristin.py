import sys
from urllib.parse import quote_plus
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

    def get_person(self, user_id):
        http_client = requests.Session()

        response = http_client.get(
            f"{self.cristin_api}/persons/{user_id}",
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

    def get_person_by_nin(self, norwegian_national_id):
        http_client = requests.Session()

        response = http_client.get(
            f"{self.cristin_api}/persons?national_id={norwegian_national_id}",
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

        return response.json()[0]

    def get_project(self, project_id):
        http_client = requests.Session()

        response = http_client.get(
            f"{self.cristin_api}/projects/{project_id}",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Basic {self.auth}",
                self.bypass_header: self.bypass_value,
            },
        )
        if not response.ok:
            print(response.text)
            return response.text

        return response.json()

    def find_project_by_title(self, title):
        http_client = requests.Session()

        response = http_client.get(
            f"{self.cristin_api}/projects?title={quote_plus(title)}",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Basic {self.auth}",
                self.bypass_header: self.bypass_value,
            },
        )

        if not response.ok:
            sys.exit(f"Error while finding project by title '{title}': {response.text}")

        projects = response.json()
        if len(projects) == 1:
            id = projects[0]["url"].split("/")[-1]
            return self.get_project(id)
        elif len(projects) == 0:
            return None
        else:
            sys.exit(f"Multiple projects found with title '{title}': {response.text}")

    def add_project(self, project):
        http_client = requests.Session()

        response = http_client.post(
            f"{self.cristin_api}/projects",
            json=project,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Basic {self.auth}",
                self.bypass_header: self.bypass_value,
            },
        )
        if not response.ok:
            print(response.text)
            return response.text

        return response.json()

    def update_project(self, project_id, project):
        http_client = requests.Session()

        project.pop("cristin_project_id", None)
        project.pop("publishable", None)
        project.pop("published", None)
        project.pop("status", None)
        project.pop("created", None)
        project.pop("last_modified", None)
        project.pop("languages", None)
        project.pop("participants_url", None)
        project.pop("creator", None)

        response = http_client.patch(
            f"{self.cristin_api}/projects/{project_id}",
            json=project,
            headers={
                "Content-Type": "application/merge-patch+json",
                "Accept": "application/json",
                "Authorization": f"Basic {self.auth}",
                self.bypass_header: self.bypass_value,
            },
        )
        if not response.ok:
            print(f"Error while updating project (ID: {project_id}): {response.text}")
            return response.text

    def update_person(self, user_id, person):
        http_client = requests.Session()

        person.pop("cristin_person_id", None)
        person.pop("norwegian_national_id", None)

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

    def put_person_image(self, user_id, image_data):
        http_client = requests.Session()

        response = http_client.put(
            f"{self.cristin_api}/persons/{user_id}/picture",
            data=image_data,
            headers={
                "Content-Type": "image/jpeg",
                "Accept": "application/json",
                "Authorization": f"Basic {self.auth}",
                self.bypass_header: self.bypass_value,
            },
        )

        if not response.ok:
            print(
                f"Error while uploading image for person (ID: {user_id}): {response.text}"
            )
            return response.text

    def _get_system_parameter(self, name):
        response = self.ssm.get_parameter(Name=name)
        return response["Parameter"]["Value"]

    def _get_secret(self, name):
        response = self.secretsmanager.get_secret_value(SecretId=name)
        secret_string = response["SecretString"]
        secret = json.loads(secret_string)
        return secret
