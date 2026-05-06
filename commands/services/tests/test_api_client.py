import json

import boto3
import responses
from moto import mock_aws

from commands.services.api_client import ApiClient

COGNITO_TOKEN_URL = "https://cognito.example.org/oauth2/token"


def _seed_aws_resources():
    ssm = boto3.client("ssm")
    ssm.put_parameter(Name="/NVA/ApiDomain", Value="api.example.org", Type="String")
    ssm.put_parameter(
        Name="/NVA/CognitoUri", Value="https://cognito.example.org", Type="String"
    )
    boto3.client("secretsmanager").create_secret(
        Name="BackendCognitoClientCredentials",
        SecretString=json.dumps(
            {"backendClientId": "id", "backendClientSecret": "secret"}
        ),
    )


@mock_aws
@responses.activate
def test_auth_header_returns_bearer_token_from_cognito():
    _seed_aws_resources()
    responses.add(
        responses.POST,
        COGNITO_TOKEN_URL,
        json={"access_token": "fresh", "expires_in": 3600},
    )

    header = ApiClient(session=boto3.Session()).auth_header()

    assert header == {"Authorization": "Bearer fresh"}


@mock_aws
@responses.activate
def test_auth_header_caches_token_until_it_expires():
    _seed_aws_resources()
    responses.add(
        responses.POST,
        COGNITO_TOKEN_URL,
        json={"access_token": "fresh", "expires_in": 3600},
    )
    client = ApiClient(session=boto3.Session())

    client.auth_header()
    client.auth_header()

    assert len(responses.calls) == 1


@mock_aws
@responses.activate
def test_auth_header_refreshes_when_token_expired():
    _seed_aws_resources()
    responses.add(
        responses.POST,
        COGNITO_TOKEN_URL,
        json={"access_token": "first", "expires_in": -1},
    )
    responses.add(
        responses.POST,
        COGNITO_TOKEN_URL,
        json={"access_token": "second", "expires_in": 3600},
    )
    client = ApiClient(session=boto3.Session())

    first = client.auth_header()
    second = client.auth_header()

    assert first == {"Authorization": "Bearer first"}
    assert second == {"Authorization": "Bearer second"}
