import json
from dataclasses import dataclass

import requests

from commands.services.api_client import ApiClient


@dataclass
class ExternalUser:
    org_abbreviation: str
    intended_purpose: str
    client_data: dict

    def save_to_file(self) -> None:
        filename = f"{self.org_abbreviation}-{self.intended_purpose}-credentials.json"
        with open(filename, "w") as json_file:
            json.dump(self.client_data, json_file, indent=4)


def create_external_user(
    client: ApiClient,
    customer_id: str,
    intended_purpose: str,
    scopes: list[str],
    shortname: str | None = None,
) -> ExternalUser:
    customer_data = _get_customer_data(client, customer_id)
    org_abbreviation = (shortname or customer_data["shortName"]).lower()

    client_data = _create_external_client_token(
        client=client,
        customer_uri=customer_data["id"],
        cristin_org_uri=customer_data["cristinId"],
        org_abbreviation=org_abbreviation,
        intended_purpose=intended_purpose,
        scopes=scopes,
    )
    return ExternalUser(org_abbreviation, intended_purpose, client_data)


def _get_customer_data(client: ApiClient, customer_id: str) -> dict:
    response = requests.get(
        f"https://{client.api_domain}/customer/{customer_id}",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            **client.auth_header(),
        },
    )
    response.raise_for_status()
    return response.json()


def _create_external_client_token(
    *,
    client: ApiClient,
    customer_uri: str,
    cristin_org_uri: str,
    org_abbreviation: str,
    intended_purpose: str,
    scopes: list[str],
) -> dict:
    request_body = {
        "clientName": f"{org_abbreviation}-{intended_purpose}-integration",
        "customerUri": customer_uri,
        "cristinOrgUri": cristin_org_uri,
        "actingUser": f"{intended_purpose}-integration@{org_abbreviation}",
        "scopes": scopes,
    }
    response = requests.post(
        f"https://{client.api_domain}/users-roles/external-clients",
        json=request_body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            **client.auth_header(),
        },
    )
    response.raise_for_status()
    response_json = response.json()
    return {
        "clientId": response_json["clientId"],
        "clientSecret": response_json["clientSecret"],
        "tokenUrl": response_json["clientUrl"],
        "clientName": request_body["clientName"],
        "customerUri": request_body["customerUri"],
        "cristinOrgUri": request_body["cristinOrgUri"],
        "actingUser": request_body["actingUser"],
        "scopes": request_body["scopes"],
    }
