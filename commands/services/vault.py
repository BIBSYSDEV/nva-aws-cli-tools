from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

DEFAULT_VAULT_ADDR = "https://vault.sikt.no:8200"
VAULT_ADDR_ENV = "VAULT_ADDR"
VAULT_TOKEN_ENV = "VAULT_TOKEN"
VAULT_TOKEN_FILE = "~/.vault-token"
VAULT_TOKEN_HEADER = "X-Vault-Token"
OIDC_LOGIN_PATH = "microsoft"
REQUEST_TIMEOUT_SECONDS = 15

KV_V2_DATA_SEGMENT = "data"
KV_V2_VERSION = "2"
MOUNT_LOOKUP_PATH = "sys/internal/ui/mounts"
TOKEN_LOOKUP_PATH = "auth/token/lookup-self"


class VaultError(Exception):
    pass


class VaultTokenRejectedError(VaultError):
    def __init__(self, message: str, status_code: int = 403) -> None:
        super().__init__(message)
        self.status_code = status_code


def read_cached_token() -> str | None:
    token = os.environ.get(VAULT_TOKEN_ENV)
    if token and token.strip():
        return token.strip()
    token_file = Path(VAULT_TOKEN_FILE).expanduser()
    if token_file.is_file():
        cached = token_file.read_text(encoding="utf-8").strip()
        return cached or None
    return None


class VaultClient:
    def __init__(self, address: str | None = None, token: str | None = None) -> None:
        self.address = (
            address or os.environ.get(VAULT_ADDR_ENV) or DEFAULT_VAULT_ADDR
        ).rstrip("/")
        self.http_client = requests.Session()
        self.token = token or read_cached_token()

    def read_secret(self, path: str) -> dict[str, Any]:
        logical_path = path.strip("/")
        if not self.token:
            raise VaultError(f"No Vault token found.\n{self.login_instructions()}")
        try:
            return self._read_any(logical_path)
        except VaultTokenRejectedError as denial:
            raise VaultError(self._explain_denial(logical_path, denial)) from denial

    def login_instructions(self) -> str:
        return (
            "Log in with the Vault CLI:\n"
            "    brew tap hashicorp/tap && brew install hashicorp/tap/vault\n"
            f"    vault login -method=oidc -path={OIDC_LOGIN_PATH} -address={self.address}\n"
            f"Or copy a token from {self.address}/ui (user menu) and run:\n"
            f"    export {VAULT_TOKEN_ENV}=<the token>"
        )

    def _explain_denial(
        self, logical_path: str, denial: VaultTokenRejectedError
    ) -> str:
        if not self._token_is_valid():
            return (
                f"Your Vault token is expired or invalid ({denial}).\n"
                f"{self.login_instructions()}"
            )
        return (
            f"Your Vault token is valid, but it has no access to {logical_path!r} "
            f"({denial}). Ask for membership in the group that grants it."
        )

    def _token_is_valid(self) -> bool:
        return self._look_up_token() is not None

    def _look_up_token(self) -> dict[str, Any] | None:
        try:
            response = self.http_client.get(
                f"{self.address}/v1/{TOKEN_LOOKUP_PATH}",
                headers={VAULT_TOKEN_HEADER: self.token or ""},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as error:
            logger.debug("Could not look up the Vault token: %s", error)
            return None
        if not response.ok:
            return None
        data = response.json().get("data")
        return data if isinstance(data, dict) else {}

    def _resolve_api_path(self, logical_path: str) -> str | None:
        try:
            response = self.http_client.get(
                f"{self.address}/v1/{MOUNT_LOOKUP_PATH}/{logical_path}",
                headers={VAULT_TOKEN_HEADER: self.token or ""},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as error:
            logger.debug("Could not look up the Vault mount: %s", error)
            return None
        if not response.ok:
            return None
        mount_data = response.json().get("data")
        if not isinstance(mount_data, dict):
            return None
        mount = str(mount_data.get("path") or "").strip("/")
        if not mount or not logical_path.startswith(mount):
            return None
        if self._kv_version(mount_data) != KV_V2_VERSION:
            return logical_path
        rest = logical_path[len(mount) :].strip("/")
        logger.debug("Vault mount %r is KV v2", mount)
        return f"{mount}/{KV_V2_DATA_SEGMENT}/{rest}"

    @staticmethod
    def _kv_version(mount_data: dict[str, Any]) -> str:
        options = mount_data.get("options")
        if not isinstance(options, dict):
            return ""
        return str(options.get("version") or "")

    def _read_any(self, logical_path: str) -> dict[str, Any]:
        attempts: list[str] = []
        denied = False
        api_paths, source = self._api_paths(logical_path)
        for api_path in api_paths:
            try:
                secret = self._read(api_path)
            except VaultTokenRejectedError as denial:
                attempts.append(f"  {api_path} -> {denial.status_code}")
                denied = True
                continue
            if secret is not None:
                return secret
            attempts.append(f"  {api_path} -> 404")
        report = f"Tried {source}:\n" + "\n".join(attempts)
        if denied:
            raise VaultTokenRejectedError(
                f"Vault denied access to {logical_path!r}.\n{report}"
            )
        raise VaultError(
            f"Secret {logical_path!r} not found in Vault at {self.address}.\n{report}"
        )

    def _read(self, api_path: str) -> dict[str, Any] | None:
        url = f"{self.address}/v1/{api_path}"
        try:
            response = self.http_client.get(
                url,
                headers={VAULT_TOKEN_HEADER: self.token or ""},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as error:
            raise VaultError(
                f"Could not reach Vault at {self.address}: {error}"
            ) from error
        if response.status_code == 404:
            logger.debug("Vault returned 404 for %s", url)
            return None
        if response.status_code in (401, 403):
            raise VaultTokenRejectedError(
                f"Vault denied access to {api_path!r} ({response.status_code})",
                response.status_code,
            )
        if not response.ok:
            raise VaultError(
                f"Vault request failed ({response.status_code}): {response.text}"
            )
        return self._unwrap(response.json())

    def _api_paths(self, logical_path: str) -> tuple[list[str], str]:
        resolved = self._resolve_api_path(logical_path)
        if resolved:
            return [resolved], "the mount Vault reported"
        return self._candidate_paths(logical_path), (
            "guessed paths, because the mount could not be looked up"
        )

    @staticmethod
    def _candidate_paths(logical_path: str) -> list[str]:
        segments = logical_path.split("/")
        if KV_V2_DATA_SEGMENT in segments:
            return [logical_path]
        with_data_segment = [
            "/".join([*segments[:depth], KV_V2_DATA_SEGMENT, *segments[depth:]])
            for depth in range(1, len(segments))
        ]
        return [*with_data_segment, logical_path]

    @staticmethod
    def _unwrap(payload: dict[str, Any]) -> dict[str, Any]:
        data = payload.get("data", {})
        nested = data.get(KV_V2_DATA_SEGMENT)
        return nested if isinstance(nested, dict) else data
