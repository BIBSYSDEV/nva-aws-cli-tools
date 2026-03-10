import http.server
import json
import socketserver
from urllib.parse import urlparse

import requests as http_requests

from commands.services.sws import SwsService

CLUSTER_INFO = {
    "name": "sws-proxy",
    "cluster_name": "sws",
    "cluster_uuid": "_na_",
    "version": {
        "number": "7.17.0",
        "build_flavor": "default",
        "build_type": "docker",
        "build_hash": "unknown",
        "build_date": "unknown",
        "build_snapshot": False,
        "lucene_version": "8.11.1",
        "minimum_wire_compatibility_version": "6.8.0",
        "minimum_index_compatibility_version": "6.0.0-beta1",
    },
    "tagline": "You Know, for Search",
}

CLUSTER_HEALTH = {
    "cluster_name": "sws",
    "status": "green",
    "timed_out": False,
    "number_of_nodes": 1,
    "number_of_data_nodes": 1,
    "active_primary_shards": 0,
    "active_shards": 0,
    "relocating_shards": 0,
    "initializing_shards": 0,
    "unassigned_shards": 0,
    "delayed_unassigned_shards": 0,
    "number_of_pending_tasks": 0,
    "number_of_in_flight_fetch": 0,
    "task_max_waiting_in_queue_millis": 0,
    "active_shards_percent_as_number": 100.0,
}

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, HEAD, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
    "Access-Control-Max-Age": "86400",
}


class SwsProxyHandler(http.server.BaseHTTPRequestHandler):
    def __init__(self, service: SwsService, indices: list[str], *args, **kwargs):
        self.service = service
        self.indices = indices
        super().__init__(*args, **kwargs)

    def _add_cors_headers(self) -> None:
        for key, value in CORS_HEADERS.items():
            self.send_header(key, value)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self._add_cors_headers()
        self.end_headers()

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/":
            self._send_json(200, CLUSTER_INFO)
        elif path == "/_cluster/health":
            self._send_json(200, CLUSTER_HEALTH)
        elif path.startswith("/_cat/indices"):
            self._handle_cat_indices()
        else:
            self._forward_request()

    def _handle_cat_indices(self) -> None:
        self._send_json(200, [{"index": name} for name in self.indices])

    def _send_json(self, status: int, body: dict) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self._add_cors_headers()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(payload)

    def _forward_request(self) -> None:
        try:
            url = f"{self.service.api_endpoint}{self.path}"

            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length) if content_length > 0 else None

            response = self._do_request(url, body)

            if response.status_code == 401:
                self.service.clear_token()
                response = self._do_request(url, body)

            self.send_response(response.status_code)
            self._add_cors_headers()
            if "Content-Type" in response.headers:
                self.send_header("Content-Type", response.headers["Content-Type"])
            self.end_headers()

            if self.command != "HEAD":
                self.wfile.write(response.content)

        except Exception as exc:
            self.send_response(502)
            self._add_cors_headers()
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(exc)}).encode())

    def _do_request(self, url: str, body: bytes | None) -> http_requests.Response:
        token = self.service._get_access_token()
        forward_headers = {"Authorization": f"Bearer {token}"}
        if self.headers.get("Content-Type"):
            forward_headers["Content-Type"] = self.headers["Content-Type"]
        return http_requests.request(
            method=self.command,
            url=url,
            headers=forward_headers,
            data=body,
        )

    do_POST = _forward_request
    do_PUT = _forward_request
    do_DELETE = _forward_request
    do_HEAD = _forward_request

    def log_message(self, format_str: str, *args) -> None:
        status = args[1] if len(args) > 1 else "-"
        print(f"  {self.command} {self.path} → {status}")


class LocalTCPServer(socketserver.TCPServer):
    allow_reuse_address = True
