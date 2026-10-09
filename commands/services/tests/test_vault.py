import pytest
import responses

from commands.services.vault import VaultClient, VaultError

VAULT_ADDRESS = "https://vault.example.no:8200"
SECRET_PATH = "service/cristin/database/test"
KV_V2_URL = f"{VAULT_ADDRESS}/v1/service/data/cristin/database/test"
KV_V1_URL = f"{VAULT_ADDRESS}/v1/service/cristin/database/test"


def build_client() -> VaultClient:
    return VaultClient(
        address=VAULT_ADDRESS, token="token", allow_interactive_login=False
    )


@responses.activate
def test_read_secret_unwraps_kv_v2_payload():
    responses.get(
        KV_V2_URL,
        json={"data": {"data": {"username": "frida", "password": "secret"}}},
        status=200,
    )

    secret = build_client().read_secret(SECRET_PATH)

    assert secret == {"username": "frida", "password": "secret"}


@responses.activate
def test_read_secret_falls_back_to_kv_v1_path():
    responses.get(KV_V2_URL, json={"errors": []}, status=404)
    responses.get(KV_V1_URL, json={"data": {"username": "frida"}}, status=200)

    secret = build_client().read_secret(SECRET_PATH)

    assert secret == {"username": "frida"}


@responses.activate
def test_read_secret_reports_missing_secret():
    responses.get(KV_V2_URL, json={"errors": []}, status=404)
    responses.get(KV_V1_URL, json={"errors": []}, status=404)

    with pytest.raises(VaultError, match="not found"):
        build_client().read_secret(SECRET_PATH)


@responses.activate
def test_denied_token_without_interactive_login_raises():
    responses.get(KV_V2_URL, json={"errors": ["permission denied"]}, status=403)

    with pytest.raises(VaultError, match="rejected the token"):
        build_client().read_secret(SECRET_PATH)


def test_missing_token_without_interactive_login_raises(monkeypatch, tmp_path):
    monkeypatch.delenv("VAULT_TOKEN", raising=False)
    monkeypatch.setattr(
        "commands.services.vault.VAULT_TOKEN_FILE", str(tmp_path / "no-token")
    )

    client = VaultClient(address=VAULT_ADDRESS, allow_interactive_login=False)

    with pytest.raises(VaultError, match="No Vault token available"):
        client.read_secret(SECRET_PATH)


@responses.activate
def test_oidc_login_exchanges_callback_for_token(monkeypatch):
    responses.post(
        f"{VAULT_ADDRESS}/v1/auth/oidc/oidc/auth_url",
        json={"data": {"auth_url": "https://login.example.no/authorize"}},
        status=200,
    )
    responses.get(
        f"{VAULT_ADDRESS}/v1/auth/oidc/oidc/callback",
        json={"auth": {"client_token": "fresh-token"}},
        status=200,
    )
    client = VaultClient(address=VAULT_ADDRESS, token="expired")
    monkeypatch.setattr(
        VaultClient,
        "_await_callback",
        lambda self, auth_url: {"code": ["abc"], "state": ["xyz"]},
    )
    monkeypatch.setattr("commands.services.vault.cache_token", lambda token: None)

    assert client.login() == "fresh-token"
    assert client.token == "fresh-token"
