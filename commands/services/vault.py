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
REQUEST_TIMEOUT_SECONDS = 15

KV_V2_DATA_SEGMENT = "data"
TOKEN_LOOKUP_PATH = "auth/token/lookup-self"


class VaultError(Exception):
    pass


class VaultTokenRejectedError(VaultError):
    pass


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
            "Log in with the Vault CLI (install it with `brew install vault`):\n"
            f"    vault login -method=oidc -address={self.address}\n"
            f"Or copy a token from {self.address}/ui and run:\n"
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
        try:
            response = self.http_client.get(
                f"{self.address}/v1/{TOKEN_LOOKUP_PATH}",
                headers={VAULT_TOKEN_HEADER: self.token or ""},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as error:
            logger.debug("Could not look up the Vault token: %s", error)
            return False
        return response.ok

    def _read_any(self, logical_path: str) -> dict[str, Any]:
        denials: list[VaultTokenRejectedError] = []
        for api_path in self._candidate_paths(logical_path):
            try:
                secret = self._read(api_path)
            except VaultTokenRejectedError as denial:
                denials.append(denial)
                continue
            if secret is not None:
                return secret
        if denials:
            raise denials[0]
        raise VaultError(
            f"Secret {logical_path!r} not found in Vault at {self.address}. "
            "Check the path and that your token has access to it."
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
                f"Vault denied access to {api_path!r} ({response.status_code})"
            )
        if not response.ok:
            raise VaultError(
                f"Vault request failed ({response.status_code}): {response.text}"
            )
        return self._unwrap(response.json())

    @staticmethod
    def _candidate_paths(logical_path: str) -> list[str]:
        segments = logical_path.split("/")
        if len(segments) > 1 and segments[1] != KV_V2_DATA_SEGMENT:
            return [
                "/".join([segments[0], KV_V2_DATA_SEGMENT, *segments[1:]]),
                logical_path,
            ]
        return [logical_path]

    @staticmethod
    def _unwrap(payload: dict[str, Any]) -> dict[str, Any]:
        data = payload.get("data", {})
        nested = data.get(KV_V2_DATA_SEGMENT)
        return nested if isinstance(nested, dict) else data
