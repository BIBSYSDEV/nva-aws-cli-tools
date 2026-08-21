import json
import logging
from dataclasses import dataclass
from enum import Enum

import boto3
from botocore.config import Config

logger = logging.getLogger(__name__)

CLOUDFORMATION_LOGICAL_ID = "ManuallyUpdatePublicationsHandler"
LOGICAL_ID_TAG = "aws:cloudformation:logical-id"
LAMBDA_RESOURCE_TYPE = "lambda:function"
DEFAULT_LIMIT = 10_000
DEFAULT_PAGE_SIZE = 100
LAMBDA_TIMEOUT_SECONDS = 900
INVOKE_READ_TIMEOUT_SECONDS = LAMBDA_TIMEOUT_SECONDS + 10
INVOKE_CONNECT_TIMEOUT_SECONDS = 10


class ManualUpdateType(str, Enum):
    PUBLISHER = "PUBLISHER"
    LICENSE = "LICENSE"
    SERIAL_PUBLICATION = "SERIAL_PUBLICATION"
    UNCONFIRMED_PUBLISHER = "UNCONFIRMED_PUBLISHER"
    UNCONFIRMED_SERIES = "UNCONFIRMED_SERIES"
    UNCONFIRMED_JOURNAL = "UNCONFIRMED_JOURNAL"
    CONTRIBUTOR_IDENTIFIER = "CONTRIBUTOR_IDENTIFIER"
    CONTRIBUTOR_AFFILIATION = "CONTRIBUTOR_AFFILIATION"
    PROJECT = "PROJECT"


class Comparator(str, Enum):
    MATCHES = "MATCHES"
    CONTAINS = "CONTAINS"


class ManualUpdateError(Exception):
    pass


@dataclass
class ManualUpdateRequest:
    type: ManualUpdateType
    old_value: str
    new_value: str
    search_params: dict[str, str]
    comparator: Comparator | None = None
    limit: int | None = None
    page_size: int | None = None

    def to_payload(self, dry_run: bool) -> dict:
        payload: dict = {
            "type": self.type.value,
            "oldValue": self.old_value,
            "newValue": self.new_value,
            "searchParams": self.search_params,
            "dryRun": dry_run,
        }
        if self.comparator is not None:
            payload["comparator"] = self.comparator.value
        if self.limit is not None:
            payload["limit"] = self.limit
        if self.page_size is not None:
            payload["pageSize"] = self.page_size
        return payload


@dataclass
class ManualUpdateService:
    session: boto3.Session
    _function_name: str | None = None

    def dry_run(self, request: ManualUpdateRequest) -> dict:
        return self._invoke(request.to_payload(dry_run=True))

    def apply(self, request: ManualUpdateRequest) -> dict:
        return self._invoke(request.to_payload(dry_run=False))

    def _invoke(self, payload: dict) -> dict:
        response = self._lambda_client().invoke(
            FunctionName=self._resolve_function_name(),
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )
        return self._parse_response(response)

    def _lambda_client(self):
        config = Config(
            read_timeout=INVOKE_READ_TIMEOUT_SECONDS,
            connect_timeout=INVOKE_CONNECT_TIMEOUT_SECONDS,
            retries={"max_attempts": 0},
        )
        return self.session.client("lambda", config=config)

    def _resolve_function_name(self) -> str:
        if self._function_name is not None:
            return self._function_name
        matches = self._find_by_logical_id()
        if not matches:
            raise ManualUpdateError(
                f"No Lambda function tagged {LOGICAL_ID_TAG}={CLOUDFORMATION_LOGICAL_ID} "
                "found in this account"
            )
        if len(matches) > 1:
            raise ManualUpdateError(
                f"Multiple Lambda functions tagged {LOGICAL_ID_TAG}={CLOUDFORMATION_LOGICAL_ID}, "
                f"cannot pick one: {matches}"
            )
        self._function_name = matches[0]
        return self._function_name

    def _find_by_logical_id(self) -> list[str]:
        client = self.session.client("resourcegroupstaggingapi")
        paginator = client.get_paginator("get_resources")
        pages = paginator.paginate(
            TagFilters=[{"Key": LOGICAL_ID_TAG, "Values": [CLOUDFORMATION_LOGICAL_ID]}],
            ResourceTypeFilters=[LAMBDA_RESOURCE_TYPE],
        )
        return [
            _function_name_from_arn(mapping["ResourceARN"])
            for page in pages
            for mapping in page["ResourceTagMappingList"]
        ]

    def _parse_response(self, response) -> dict:
        payload_bytes = response["Payload"].read()
        body = payload_bytes.decode("utf-8") if payload_bytes else ""
        if response.get("FunctionError"):
            raise ManualUpdateError(_format_lambda_error(body))
        try:
            return json.loads(body)
        except json.JSONDecodeError as error:
            raise ManualUpdateError(
                f"Could not parse Lambda response as JSON: {body}"
            ) from error


def _function_name_from_arn(arn: str) -> str:
    return arn.rsplit(":function:", 1)[-1].split(":")[0]


def _format_lambda_error(body: str) -> str:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return f"Lambda returned an error: {body}"
    message = parsed.get("errorMessage", body)
    error_type = parsed.get("errorType")
    return f"{error_type}: {message}" if error_type else message
