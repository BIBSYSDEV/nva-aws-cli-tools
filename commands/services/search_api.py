import boto3
import requests


class SearchApiService:
    def __init__(self, profile):
        self.session = boto3.Session(profile_name=profile)
        self.ssm = self.session.client("ssm")
        self.api_domain = self._get_system_parameter("/NVA/ApiDomain")

    def _get_system_parameter(self, name):
        response = self.ssm.get_parameter(Name=name)
        return response["Parameter"]["Value"]

    def get_uri(self, type):
        return f"https://{self.api_domain}/search/{type}"

    def resource_search(self, queryParameters):
        url = f"{self.get_uri("resources")}"
        headers = {
            "Accept": "application/json; version=2024-12-01",
        }
        response = requests.get(url, headers=headers, params=queryParameters)
        if response.status_code == 200:  # If the status code indicates success
            return response.json()
        else:
            print(
                f"Failed to search. {response.status_code}: {response.json()}",
                response.status_code,
            )
            return None
