import pytest
import responses

from commands.services.vault import VaultClient, VaultError

VAULT_ADDRESS = "https://vault.example.no:8200"
SECRET_PATH = "service/cristin/database/test"
KV_V2_URL = f"{VAULT_ADDRESS}/v1/service/data/cristin/database/test"
KV_V1_URL = f"{VAULT_ADDRESS}/v1/service/cristin/database/test"
TOKEN_LOOKUP_URL = f"{VAULT_ADDRESS}/v1/auth/token/lookup-self"
TOKEN_RENEW_URL = f"{VAULT_ADDRESS}/v1/auth/token/renew-self"


def build_client() -> VaultClient:
    return VaultClient(address=VAULT_ADDRESS, token="token")


@responses.activate
def test_read_secret_unwraps_kv_v2_payload():
    responses.get(
        KV_V2_URL,
        json={"data": {"data": {"FRIDA": "frida-password"}}},
        status=200,
    )

    secret = build_client().read_secret(SECRET_PATH)

    assert secret == {"FRIDA": "frida-password"}


@responses.activate
def test_read_secret_falls_back_to_kv_v1_path():
    responses.get(KV_V2_URL, json={"errors": []}, status=404)
    responses.get(KV_V1_URL, json={"data": {"FRIDA": "frida-password"}}, status=200)

    secret = build_client().read_secret(SECRET_PATH)

    assert secret == {"FRIDA": "frida-password"}


@responses.activate
def test_denied_kv_v2_probe_still_falls_back_to_kv_v1():
    responses.get(KV_V2_URL, json={"errors": ["permission denied"]}, status=403)
    responses.get(KV_V1_URL, json={"data": {"FRIDA": "frida-password"}}, status=200)

    secret = build_client().read_secret(SECRET_PATH)

    assert secret == {"FRIDA": "frida-password"}


@responses.activate
def test_a_token_close_to_expiry_is_renewed_before_reading():
    responses.get(
        TOKEN_LOOKUP_URL, json={"data": {"renewable": True, "ttl": 60}}, status=200
    )
    renewal = responses.post(TOKEN_RENEW_URL, json={"auth": {}}, status=200)
    responses.get(KV_V2_URL, json={"data": {"data": {"FRIDA": "pw"}}}, status=200)

    build_client().read_secret(SECRET_PATH)

    assert renewal.call_count == 1


@responses.activate
def test_a_token_with_plenty_of_time_left_is_not_renewed():
    responses.get(
        TOKEN_LOOKUP_URL, json={"data": {"renewable": True, "ttl": 3600}}, status=200
    )
    renewal = responses.post(TOKEN_RENEW_URL, json={"auth": {}}, status=200)
    responses.get(KV_V2_URL, json={"data": {"data": {"FRIDA": "pw"}}}, status=200)

    build_client().read_secret(SECRET_PATH)

    assert renewal.call_count == 0


@responses.activate
def test_a_failed_renewal_does_not_stop_the_read():
    responses.get(
        TOKEN_LOOKUP_URL, json={"data": {"renewable": True, "ttl": 60}}, status=200
    )
    responses.post(TOKEN_RENEW_URL, json={"errors": ["no"]}, status=403)
    responses.get(KV_V2_URL, json={"data": {"data": {"FRIDA": "pw"}}}, status=200)

    assert build_client().read_secret(SECRET_PATH) == {"FRIDA": "pw"}


@responses.activate
def test_read_secret_reports_missing_secret():
    responses.get(KV_V2_URL, json={"errors": []}, status=404)
    responses.get(KV_V1_URL, json={"errors": []}, status=404)

    with pytest.raises(VaultError, match="not found"):
        build_client().read_secret(SECRET_PATH)


def test_missing_token_explains_how_to_log_in(monkeypatch, tmp_path):
    monkeypatch.delenv("VAULT_TOKEN", raising=False)
    monkeypatch.setattr(
        "commands.services.vault.VAULT_TOKEN_FILE", str(tmp_path / "no-token")
    )

    client = VaultClient(address=VAULT_ADDRESS)

    with pytest.raises(VaultError, match="vault login -method=oidc") as error:
        client.read_secret(SECRET_PATH)

    assert "No Vault token found" in str(error.value)
    assert VAULT_ADDRESS in str(error.value)


@responses.activate
def test_expired_token_is_reported_as_a_login_problem():
    responses.get(KV_V2_URL, json={"errors": ["permission denied"]}, status=403)
    responses.get(KV_V1_URL, json={"errors": ["permission denied"]}, status=403)
    responses.get(TOKEN_LOOKUP_URL, json={"errors": ["permission denied"]}, status=403)

    with pytest.raises(VaultError, match="expired or invalid") as error:
        build_client().read_secret(SECRET_PATH)

    assert "vault login -method=oidc" in str(error.value)


@responses.activate
def test_valid_token_without_access_is_reported_as_a_permission_problem():
    responses.get(KV_V2_URL, json={"errors": ["permission denied"]}, status=403)
    responses.get(KV_V1_URL, json={"errors": ["permission denied"]}, status=403)
    responses.get(TOKEN_LOOKUP_URL, json={"data": {"id": "token"}}, status=200)

    with pytest.raises(VaultError, match="no access to") as error:
        build_client().read_secret(SECRET_PATH)

    assert "vault login" not in str(error.value)
