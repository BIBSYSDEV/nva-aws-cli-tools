"""Pattern: use `MagicMock` (and `patch`) for non-AWS dependencies and for
assertions about *what was not called*.

moto can't intercept `requests.post` to Cognito or the NVA API, so the OAuth
token refresh has to be stubbed at the Python level. MagicMock is also the
right tool when the test is asserting "this code performs no I/O", because
moto would happily allow the calls. For pure AWS interactions, prefer moto
(see `test_cognito_api.py`)."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from commands.services.api_client import ApiClient


def _session_with_aws():
    ssm = MagicMock()
    ssm.get_parameters.return_value = {
        "Parameters": [
            {"Name": "/NVA/ApiDomain", "Value": "api.example.org"},
            {"Name": "/NVA/CognitoUri", "Value": "https://cognito.example.org"},
        ]
    }
    secrets = MagicMock()
    secrets.get_secret_value.return_value = {
        "SecretString": '{"backendClientId": "id", "backendClientSecret": "secret"}'
    }
    session = MagicMock()
    session.client.side_effect = lambda name: {"ssm": ssm, "secretsmanager": secrets}[name]
    return session


def test_construction_does_no_io():
    """The whole point of this refactor: instantiating the client must not
    talk to AWS or Cognito, so tests can build one without mocks."""
    session = MagicMock()
    session.client.side_effect = AssertionError("session.client must not be called at construction time")

    ApiClient(session=session)


def test_api_domain_is_loaded_lazily_and_cached():
    session = _session_with_aws()
    client = ApiClient(session=session)

    assert client.api_domain == "api.example.org"
    assert client.api_domain == "api.example.org"

    session.client.assert_called_once_with("ssm")


def test_auth_header_refreshes_expired_token():
    session = _session_with_aws()
    client = ApiClient(session=session)
    client._token = "stale"
    client._token_expires_at = datetime.now() - timedelta(seconds=1)

    fake_response = MagicMock()
    fake_response.json.return_value = {"access_token": "fresh", "expires_in": 3600}
    with patch("commands.services.api_client.requests.post", return_value=fake_response) as post:
        header = client.auth_header()

    assert header == {"Authorization": "Bearer fresh"}
    post.assert_called_once()
