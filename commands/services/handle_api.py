import requests

from commands.services.api_client import ApiClient


def create_handle(client: ApiClient, request_body: dict) -> dict:
    response = requests.post(
        f"https://{client.api_domain}/handle/",
        headers={**client.auth_header(), "Content-Type": "application/json"},
        json=request_body,
    )
    return response.json()


def update_handle(
    client: ApiClient, prefix: str, suffix: str, request_body: dict
) -> dict:
    response = requests.put(
        f"https://{client.api_domain}/handle/{prefix}/{suffix}",
        headers={**client.auth_header(), "Content-Type": "application/json"},
        json=request_body,
    )
    return response.json()
