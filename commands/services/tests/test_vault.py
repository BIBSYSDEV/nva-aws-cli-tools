import pytest
import responses

from commands.services.vault import VaultClient, VaultError

VAULT_ADDRESS = "https://vault.example.no:8200"
SECRET_PATH = "service/cristin/database/test"
MOUNT_LOOKUP_URL = f"{VAULT_ADDRESS}/v1/sys/internal/ui/mounts/{SECRET_PATH}"
TOKEN_LOOKUP_URL = f"{VAULT_ADDRESS}/v1/auth/token/lookup-self"

SHALLOW_MOUNT_URL = f"{VAULT_ADDRESS}/v1/service/data/cristin/database/test"
DEEP_MOUNT_URL = f"{VAULT_ADDRESS}/v1/service/cristin/data/database/test"
RAW_URL = f"{VAULT_ADDRESS}/v1/{SECRET_PATH}"
GUESSED_URLS = (
    SHALLOW_MOUNT_URL,
    DEEP_MOUNT_URL,
    f"{VAULT_ADDRESS}/v1/service/cristin/database/data/test",
    RAW_URL,
)

SECRET = {"frida": "frida-password"}


def build_client() -> VaultClient:
    return VaultClient(address=VAULT_ADDRESS, token="token")


def mount_is(path: str, version: str) -> None:
    responses.get(
        MOUNT_LOOKUP_URL,
        json={"data": {"path": path, "options": {"version": version}}},
        status=200,
    )


def mount_lookup_unavailable() -> None:
    responses.get(MOUNT_LOOKUP_URL, json={"errors": []}, status=403)


def deny_every_guess() -> None:
    for url in GUESSED_URLS:
        responses.get(url, json={"errors": ["permission denied"]}, status=403)


@responses.activate
def test_reads_from_the_kv_v2_path_of_a_single_segment_mount():
    mount_is("service/", "2")
    responses.get(SHALLOW_MOUNT_URL, json={"data": {"data": SECRET}}, status=200)

    assert build_client().read_secret(SECRET_PATH) == SECRET


@responses.activate
def test_reads_from_the_kv_v2_path_of_a_nested_mount():
    mount_is("service/cristin/", "2")
    responses.get(DEEP_MOUNT_URL, json={"data": {"data": SECRET}}, status=200)

    assert build_client().read_secret(SECRET_PATH) == SECRET


@responses.activate
def test_reads_from_the_raw_path_of_a_kv_v1_mount():
    mount_is("service/", "1")
    responses.get(RAW_URL, json={"data": SECRET}, status=200)

    assert build_client().read_secret(SECRET_PATH) == SECRET


@responses.activate
def test_guesses_the_path_when_the_mount_cannot_be_looked_up():
    mount_lookup_unavailable()
    responses.get(SHALLOW_MOUNT_URL, json={"errors": []}, status=404)
    responses.get(DEEP_MOUNT_URL, json={"data": {"data": SECRET}}, status=200)

    assert build_client().read_secret(SECRET_PATH) == SECRET


@responses.activate
def test_a_denied_guess_does_not_stop_the_remaining_ones():
    mount_lookup_unavailable()
    responses.get(SHALLOW_MOUNT_URL, json={"errors": ["permission denied"]}, status=403)
    responses.get(DEEP_MOUNT_URL, json={"data": {"data": SECRET}}, status=200)

    assert build_client().read_secret(SECRET_PATH) == SECRET


@responses.activate
def test_read_secret_reports_missing_secret():
    mount_lookup_unavailable()
    for url in GUESSED_URLS:
        responses.get(url, json={"errors": []}, status=404)

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
    assert "-path=microsoft" in str(error.value)


@responses.activate
def test_expired_token_is_reported_as_a_login_problem():
    mount_lookup_unavailable()
    deny_every_guess()
    responses.get(TOKEN_LOOKUP_URL, json={"errors": ["permission denied"]}, status=403)

    with pytest.raises(VaultError, match="expired or invalid") as error:
        build_client().read_secret(SECRET_PATH)

    assert "vault login -method=oidc" in str(error.value)


@responses.activate
def test_valid_token_without_access_is_reported_as_a_permission_problem():
    mount_lookup_unavailable()
    deny_every_guess()
    responses.get(TOKEN_LOOKUP_URL, json={"data": {"id": "token"}}, status=200)

    with pytest.raises(VaultError, match="no access to") as error:
        build_client().read_secret(SECRET_PATH)

    assert "vault login" not in str(error.value)
