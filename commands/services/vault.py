from __future__ import annotations

import logging
import os
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import parse_qs, urlparse

import requests

logger = logging.getLogger(__name__)

DEFAULT_VAULT_ADDR = "https://vault.sikt.no:8200"
VAULT_ADDR_ENV = "VAULT_ADDR"
VAULT_TOKEN_ENV = "VAULT_TOKEN"
VAULT_TOKEN_FILE = "~/.vault-token"
VAULT_TOKEN_HEADER = "X-Vault-Token"
REQUEST_TIMEOUT_SECONDS = 15

OIDC_MOUNT_ENV = "VAULT_OIDC_MOUNT"
OIDC_ROLE_ENV = "VAULT_OIDC_ROLE"
DEFAULT_OIDC_MOUNT = "oidc"
CALLBACK_HOST = "localhost"
CALLBACK_PORT = 8250
CALLBACK_PATH = "/oidc/callback"
LOGIN_TIMEOUT_SECONDS = 180

KV_V2_DATA_SEGMENT = "data"

BROWSER_RESPONSE_BODY = (
    b"<html><body><h2>Vault-innlogging fullfort</h2>"
    b"<p>Du kan lukke dette vinduet.</p></body></html>"
)


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


def cache_token(token: str) -> None:
    token_file = Path(VAULT_TOKEN_FILE).expanduser()
    try:
        token_file.write_text(token, encoding="utf-8")
        token_file.chmod(0o600)
    except OSError as error:
        logger.debug("Could not cache Vault token in %s: %s", token_file, error)


class _CallbackHandler(BaseHTTPRequestHandler):
    query_parameters: ClassVar[dict[str, list[str]]] = {}

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != CALLBACK_PATH:
            self.send_error(404)
            return
        type(self).query_parameters = parse_qs(parsed.query)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(BROWSER_RESPONSE_BODY)

    def log_message(self, format: str, *args: Any) -> None:
        logger.debug("OIDC callback: %s", format % args)


class VaultClient:
    def __init__(
        self,
        address: str | None = None,
        token: str | None = None,
        allow_interactive_login: bool = True,
    ) -> None:
        self.address = (
            address or os.environ.get(VAULT_ADDR_ENV) or DEFAULT_VAULT_ADDR
        ).rstrip("/")
        self.allow_interactive_login = allow_interactive_login
        self.http_client = requests.Session()
        self.token = token or read_cached_token()

    def read_secret(self, path: str) -> dict[str, Any]:
        logical_path = path.strip("/")
        if not self.token:
            self.login()
        try:
            return self._read_any(logical_path)
        except VaultTokenRejectedError:
            if not self.allow_interactive_login:
                raise VaultError(
                    "Vault rejected the token. Log in with `vault login -method=oidc` and try again."
                )
            self.login()
            return self._read_any(logical_path)

    def login(self) -> str:
        if not self.allow_interactive_login:
            raise VaultError(
                f"No Vault token available. Set {VAULT_TOKEN_ENV} or log in with "
                "`vault login -method=oidc`."
            )
        token = self._oidc_login()
        self.token = token
        cache_token(token)
        return token

    def _oidc_login(self) -> str:
        mount = os.environ.get(OIDC_MOUNT_ENV) or DEFAULT_OIDC_MOUNT
        role = os.environ.get(OIDC_ROLE_ENV)
        redirect_uri = f"http://{CALLBACK_HOST}:{CALLBACK_PORT}{CALLBACK_PATH}"
        payload: dict[str, Any] = {"redirect_uri": redirect_uri}
        if role:
            payload["role"] = role
        auth_url = self._request_auth_url(mount, payload)
        parameters = self._await_callback(auth_url)
        return self._exchange_callback(mount, parameters)

    def _request_auth_url(self, mount: str, payload: dict[str, Any]) -> str:
        response = self._post(f"auth/{mount}/oidc/auth_url", payload)
        auth_url = response.get("data", {}).get("auth_url")
        if not auth_url:
            raise VaultError(
                f"Vault did not return an OIDC auth_url for mount {mount!r}. "
                f"Set {OIDC_MOUNT_ENV}/{OIDC_ROLE_ENV} if the defaults are wrong."
            )
        return auth_url

    def _await_callback(self, auth_url: str) -> dict[str, list[str]]:
        _CallbackHandler.query_parameters = {}
        try:
            server = HTTPServer((CALLBACK_HOST, CALLBACK_PORT), _CallbackHandler)
        except OSError as error:
            raise VaultError(
                f"Could not listen on {CALLBACK_HOST}:{CALLBACK_PORT} for the Vault login callback: {error}"
            )
        server.timeout = LOGIN_TIMEOUT_SECONDS
        logger.info("Opening browser for Vault login")
        print(f"Logg inn i nettleseren hvis den ikke åpner seg: {auth_url}")
        browser_thread = threading.Thread(
            target=webbrowser.open, args=(auth_url,), daemon=True
        )
        browser_thread.start()
        with server:
            server.handle_request()
        parameters = _CallbackHandler.query_parameters
        if not parameters.get("code") or not parameters.get("state"):
            raise VaultError("Vault login was not completed in the browser")
        return parameters

    def _exchange_callback(self, mount: str, parameters: dict[str, list[str]]) -> str:
        url = f"{self.address}/v1/auth/{mount}/oidc/callback"
        query = {
            "code": parameters["code"][0],
            "state": parameters["state"][0],
        }
        try:
            response = self.http_client.get(
                url, params=query, timeout=REQUEST_TIMEOUT_SECONDS
            )
        except requests.RequestException as error:
            raise VaultError(f"Could not reach Vault at {self.address}: {error}")
        if not response.ok:
            raise VaultError(
                f"Vault login failed ({response.status_code}): {response.text}"
            )
        token = response.json().get("auth", {}).get("client_token")
        if not token:
            raise VaultError("Vault login response contained no client token")
        return token

    def _read_any(self, logical_path: str) -> dict[str, Any]:
        for api_path in self._candidate_paths(logical_path):
            secret = self._read(api_path)
            if secret is not None:
                return secret
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
            raise VaultError(f"Could not reach Vault at {self.address}: {error}")
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

    def _post(self, api_path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.address}/v1/{api_path}"
        try:
            response = self.http_client.post(
                url, json=payload, timeout=REQUEST_TIMEOUT_SECONDS
            )
        except requests.RequestException as error:
            raise VaultError(f"Could not reach Vault at {self.address}: {error}")
        if not response.ok:
            raise VaultError(
                f"Vault request to {api_path} failed ({response.status_code}): {response.text}"
            )
        return response.json()

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
