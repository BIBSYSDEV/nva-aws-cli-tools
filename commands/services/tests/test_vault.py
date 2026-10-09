import stat
import threading
import time

import pytest
import requests
import responses

from commands.services import vault as vault_module
from commands.services.vault import VaultClient, VaultError, cache_token

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
    responses.get(KV_V1_URL, json={"errors": ["permission denied"]}, status=403)

    with pytest.raises(VaultError, match="rejected the token"):
        build_client().read_secret(SECRET_PATH)


@responses.activate
def test_denied_kv_v2_probe_still_falls_back_to_kv_v1():
    responses.get(KV_V2_URL, json={"errors": ["permission denied"]}, status=403)
    responses.get(KV_V1_URL, json={"data": {"username": "frida"}}, status=200)

    secret = build_client().read_secret(SECRET_PATH)

    assert secret == {"username": "frida"}


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


def test_cached_token_file_is_only_readable_by_the_user(monkeypatch, tmp_path):
    token_file = tmp_path / "token"
    token_file.write_text("old", encoding="utf-8")
    token_file.chmod(0o644)
    monkeypatch.setattr("commands.services.vault.VAULT_TOKEN_FILE", str(token_file))

    cache_token("fresh-token")

    assert token_file.read_text(encoding="utf-8") == "fresh-token"
    assert stat.S_IMODE(token_file.stat().st_mode) == 0o600


def call_callback(port: int, state: str, code: str) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            requests.get(
                f"http://localhost:{port}/oidc/callback",
                params={"code": code, "state": state},
                timeout=2,
            )
            return
        except requests.RequestException:
            time.sleep(0.05)


def await_callback_in_thread(client: VaultClient, auth_url: str) -> dict:
    result: dict = {}

    def run() -> None:
        try:
            result["parameters"] = client._await_callback(auth_url)
        except VaultError as error:
            result["error"] = error

    thread = threading.Thread(target=run)
    thread.start()
    return {"thread": thread, "result": result}


def test_callback_accepts_the_state_vault_issued(monkeypatch):
    port = 18250
    monkeypatch.setattr(vault_module, "CALLBACK_PORT", port)
    monkeypatch.setattr(vault_module.webbrowser, "open", lambda url: None)
    client = VaultClient(address=VAULT_ADDRESS, token="token")

    pending = await_callback_in_thread(
        client, "https://login.example.no/a?state=expected"
    )
    call_callback(port, state="expected", code="the-code")
    pending["thread"].join(timeout=10)

    assert pending["result"]["parameters"]["code"] == ["the-code"]


def test_callback_ignores_a_request_with_another_state(monkeypatch):
    port = 18251
    monkeypatch.setattr(vault_module, "CALLBACK_PORT", port)
    monkeypatch.setattr(vault_module, "LOGIN_TIMEOUT_SECONDS", 2)
    monkeypatch.setattr(vault_module, "CALLBACK_REQUEST_TIMEOUT_SECONDS", 1)
    monkeypatch.setattr(vault_module.webbrowser, "open", lambda url: None)
    client = VaultClient(address=VAULT_ADDRESS, token="token")

    pending = await_callback_in_thread(
        client, "https://login.example.no/a?state=expected"
    )
    call_callback(port, state="forged", code="attacker-code")
    pending["thread"].join(timeout=15)

    assert "parameters" not in pending["result"]
    assert isinstance(pending["result"]["error"], VaultError)
