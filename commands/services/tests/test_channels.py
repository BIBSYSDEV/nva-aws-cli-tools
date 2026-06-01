import json

import boto3
import pytest
import responses
from click.testing import CliRunner
from moto import mock_aws

from commands.channels import _identifier_from_id, channels
from commands.services.channels_api import (
    ChannelNotFoundError,
    ChannelsApiService,
)
from commands.utils import AppContext

API_DOMAIN = "api.example.org"
COGNITO_URL = "https://cognito.example.org/oauth2/token"
SERIAL_URL = f"https://{API_DOMAIN}/publication-channels-v2/serial-publication"
PUBLISHER_URL = f"https://{API_DOMAIN}/publication-channels-v2/publisher"
JOURNAL_URL = f"https://{API_DOMAIN}/publication-channels-v2/journal"
SERIES_URL = f"https://{API_DOMAIN}/publication-channels-v2/series"
CHANNEL_URL = f"https://{API_DOMAIN}/publication-channels-v2/channel"

AN_IDENTIFIER = "151f411d-68cd-4c7a-9cbb-daf00e0326ce"


def _ctx() -> AppContext:
    return AppContext(
        log_level=0, profile=None, session=boto3.Session(region_name="eu-west-1")
    )


def _seed_aws() -> None:
    ssm = boto3.client("ssm", region_name="eu-west-1")
    ssm.put_parameter(Name="/NVA/ApiDomain", Value=API_DOMAIN, Type="String")
    ssm.put_parameter(
        Name="/NVA/CognitoUri", Value="https://cognito.example.org", Type="String"
    )
    boto3.client("secretsmanager", region_name="eu-west-1").create_secret(
        Name="BackendCognitoClientCredentials",
        SecretString=json.dumps(
            {"backendClientId": "id", "backendClientSecret": "secret"}
        ),
    )


def _add_cognito() -> None:
    responses.add(
        responses.POST, COGNITO_URL, json={"access_token": "token", "expires_in": 3600}
    )


def _a_serial_hit() -> dict:
    return {
        "id": f"https://{API_DOMAIN}/publication-channels-v2/serial-publication/{AN_IDENTIFIER}/2024",
        "type": "Journal",
        "name": "Nature",
        "printIssn": "0028-0836",
        "onlineIssn": "1476-4687",
        "year": "2024",
    }


def _a_publisher_hit() -> dict:
    return {
        "id": f"https://{API_DOMAIN}/publication-channels-v2/publisher/{AN_IDENTIFIER}/2024",
        "type": "Publisher",
        "name": "Nature Publishing Group",
        "isbnPrefix": "978-0",
        "year": "2024",
    }


@mock_aws
@responses.activate
def test_search_merges_serial_and_publisher_hits():
    _seed_aws()
    _add_cognito()
    responses.add(
        responses.GET, SERIAL_URL, json={"hits": [_a_serial_hit()], "totalHits": 1}
    )
    responses.add(
        responses.GET,
        PUBLISHER_URL,
        json={"hits": [_a_publisher_hit()], "totalHits": 1},
    )

    runner = CliRunner()
    result = runner.invoke(channels, ["search", "Nature"], obj=_ctx())

    assert result.exit_code == 0, result.output
    assert "Nature" in result.output
    assert "Publishing" in result.output
    assert "Journal" in result.output
    assert "Publisher" in result.output


@mock_aws
@responses.activate
def test_search_with_kind_publisher_only_calls_publisher_endpoint():
    _seed_aws()
    _add_cognito()
    responses.add(
        responses.GET,
        PUBLISHER_URL,
        json={"hits": [_a_publisher_hit()], "totalHits": 1},
    )

    runner = CliRunner()
    result = runner.invoke(
        channels, ["search", "Nature", "--kind", "publisher"], obj=_ctx()
    )

    assert result.exit_code == 0, result.output
    get_calls = [c for c in responses.calls if c.request.method == "GET"]
    assert len(get_calls) == 1
    assert "publisher" in get_calls[0].request.url


@mock_aws
@responses.activate
def test_get_auto_detects_serial_first():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, f"{SERIAL_URL}/{AN_IDENTIFIER}", json=_a_serial_hit())

    runner = CliRunner()
    result = runner.invoke(channels, ["get", AN_IDENTIFIER], obj=_ctx())

    assert result.exit_code == 0, result.output
    assert "Resolved kind: serial-publication" in result.output
    assert "Nature" in result.output


@mock_aws
@responses.activate
def test_get_falls_back_to_publisher_on_500_from_serial():
    _seed_aws()
    _add_cognito()
    responses.add(
        responses.GET,
        f"{SERIAL_URL}/{AN_IDENTIFIER}",
        json={"message": "Internal server error"},
        status=500,
    )
    responses.add(
        responses.GET, f"{PUBLISHER_URL}/{AN_IDENTIFIER}", json=_a_publisher_hit()
    )

    runner = CliRunner()
    result = runner.invoke(channels, ["get", AN_IDENTIFIER], obj=_ctx())

    assert result.exit_code == 0, result.output
    assert "Resolved kind: publisher" in result.output


@mock_aws
@responses.activate
def test_get_falls_back_to_publisher_on_404():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, f"{SERIAL_URL}/{AN_IDENTIFIER}", status=404)
    responses.add(
        responses.GET, f"{PUBLISHER_URL}/{AN_IDENTIFIER}", json=_a_publisher_hit()
    )

    runner = CliRunner()
    result = runner.invoke(channels, ["get", AN_IDENTIFIER], obj=_ctx())

    assert result.exit_code == 0, result.output
    assert "Resolved kind: publisher" in result.output


@mock_aws
@responses.activate
def test_get_raises_when_neither_kind_has_channel():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, f"{SERIAL_URL}/{AN_IDENTIFIER}", status=404)
    responses.add(responses.GET, f"{PUBLISHER_URL}/{AN_IDENTIFIER}", status=404)

    runner = CliRunner()
    result = runner.invoke(channels, ["get", AN_IDENTIFIER], obj=_ctx())

    assert result.exit_code != 0
    assert "no channel" in result.output.lower()


@mock_aws
@responses.activate
def test_create_with_isbn_creates_publisher():
    _seed_aws()
    _add_cognito()
    responses.add(
        responses.POST,
        PUBLISHER_URL,
        json={"id": "x", "type": "Publisher", "name": "Acme"},
        status=201,
    )

    runner = CliRunner()
    result = runner.invoke(
        channels,
        ["create", "--name", "Acme", "--isbn", "978-82-12"],
        obj=_ctx(),
    )

    assert result.exit_code == 0, result.output
    post_calls = [c for c in responses.calls if c.request.method == "POST"]
    publisher_posts = [c for c in post_calls if "publisher" in c.request.url]
    assert len(publisher_posts) == 1
    body = json.loads(publisher_posts[0].request.body)
    assert body == {"name": "Acme", "isbnPrefix": "978-82-12"}


@mock_aws
@responses.activate
def test_create_with_print_issn_creates_serial_journal_by_default():
    _seed_aws()
    _add_cognito()
    responses.add(
        responses.POST,
        SERIAL_URL,
        json={"id": "x", "type": "Journal", "name": "Foo"},
        status=201,
    )

    runner = CliRunner()
    result = runner.invoke(
        channels,
        ["create", "--name", "Foo", "--print-issn", "1234-5678"],
        obj=_ctx(),
    )

    assert result.exit_code == 0, result.output
    serial_posts = [
        c
        for c in responses.calls
        if c.request.method == "POST" and "serial" in c.request.url
    ]
    assert len(serial_posts) == 1
    body = json.loads(serial_posts[0].request.body)
    assert body["type"] == "Journal"


@mock_aws
@responses.activate
def test_create_with_kind_series_uses_series_endpoint():
    _seed_aws()
    _add_cognito()
    responses.add(
        responses.POST,
        SERIES_URL,
        json={"id": "x", "type": "Series", "name": "Bar"},
        status=201,
    )

    runner = CliRunner()
    result = runner.invoke(
        channels,
        ["create", "--name", "Bar", "--kind", "series", "--print-issn", "1234-5678"],
        obj=_ctx(),
    )

    assert result.exit_code == 0, result.output
    series_posts = [
        c
        for c in responses.calls
        if c.request.method == "POST" and c.request.url.endswith("/series")
    ]
    assert len(series_posts) == 1


def test_create_without_inferrable_kind_fails():
    runner = CliRunner()
    result = runner.invoke(channels, ["create", "--name", "Foo"], obj=_ctx())
    assert result.exit_code != 0
    assert "infer" in result.output.lower() or "kind" in result.output.lower()


@mock_aws
@responses.activate
def test_create_rejects_issn_for_publisher_kind():
    _seed_aws()
    _add_cognito()

    runner = CliRunner()
    result = runner.invoke(
        channels,
        [
            "create",
            "--name",
            "Acme",
            "--kind",
            "publisher",
            "--print-issn",
            "1234-5678",
        ],
        obj=_ctx(),
    )
    assert result.exit_code != 0
    assert "issn" in result.output.lower()


@mock_aws
@responses.activate
def test_update_auto_detects_kind_and_calls_serial_put():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, f"{SERIAL_URL}/{AN_IDENTIFIER}", json=_a_serial_hit())
    responses.add(responses.PUT, f"{SERIAL_URL}/{AN_IDENTIFIER}", status=202)

    runner = CliRunner()
    result = runner.invoke(
        channels,
        ["update", AN_IDENTIFIER, "--name", "New name"],
        obj=_ctx(),
    )

    assert result.exit_code == 0, result.output
    put_calls = [c for c in responses.calls if c.request.method == "PUT"]
    assert len(put_calls) == 1
    body = json.loads(put_calls[0].request.body)
    assert body == {"type": "UpdateSerialPublicationRequest", "name": "New name"}


@mock_aws
@responses.activate
def test_update_publisher_uses_publisher_put_when_serial_returns_404():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, f"{SERIAL_URL}/{AN_IDENTIFIER}", status=404)
    responses.add(
        responses.GET, f"{PUBLISHER_URL}/{AN_IDENTIFIER}", json=_a_publisher_hit()
    )
    responses.add(responses.PUT, f"{PUBLISHER_URL}/{AN_IDENTIFIER}", status=202)

    runner = CliRunner()
    result = runner.invoke(
        channels,
        ["update", AN_IDENTIFIER, "--name", "Acme", "--isbn", "978-1"],
        obj=_ctx(),
    )

    assert result.exit_code == 0, result.output
    put_calls = [c for c in responses.calls if c.request.method == "PUT"]
    assert len(put_calls) == 1
    body = json.loads(put_calls[0].request.body)
    assert body == {"type": "UpdatePublisherRequest", "name": "Acme", "isbn": "978-1"}


def test_update_requires_at_least_one_field():
    runner = CliRunner()
    result = runner.invoke(channels, ["update", AN_IDENTIFIER], obj=_ctx())
    assert result.exit_code != 0
    assert "at least one" in result.output.lower()


@mock_aws
@responses.activate
def test_update_rejects_isbn_for_serial_channel():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, f"{SERIAL_URL}/{AN_IDENTIFIER}", json=_a_serial_hit())

    runner = CliRunner()
    result = runner.invoke(
        channels,
        ["update", AN_IDENTIFIER, "--isbn", "978-1"],
        obj=_ctx(),
    )

    assert result.exit_code != 0
    assert "isbn" in result.output.lower()
    put_calls = [c for c in responses.calls if c.request.method == "PUT"]
    assert len(put_calls) == 0


@mock_aws
@responses.activate
def test_fetch_auto_raises_when_not_found():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, f"{SERIAL_URL}/{AN_IDENTIFIER}", status=404)
    responses.add(responses.GET, f"{PUBLISHER_URL}/{AN_IDENTIFIER}", status=404)

    service = ChannelsApiService(None)
    with pytest.raises(ChannelNotFoundError):
        service.fetch_auto(AN_IDENTIFIER)


@mock_aws
@responses.activate
def test_search_kind_journal_calls_journal_endpoint_only():
    _seed_aws()
    _add_cognito()
    responses.add(
        responses.GET, JOURNAL_URL, json={"hits": [_a_serial_hit()], "totalHits": 1}
    )

    runner = CliRunner()
    result = runner.invoke(
        channels, ["search", "Nature", "--kind", "journal"], obj=_ctx()
    )

    assert result.exit_code == 0, result.output
    get_calls = [c for c in responses.calls if c.request.method == "GET"]
    assert len(get_calls) == 1
    assert get_calls[0].request.url.startswith(JOURNAL_URL)


@mock_aws
@responses.activate
def test_delete_calls_channel_endpoint_after_confirmation():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, f"{SERIAL_URL}/{AN_IDENTIFIER}", json=_a_serial_hit())
    responses.add(responses.DELETE, f"{CHANNEL_URL}/{AN_IDENTIFIER}", status=204)

    runner = CliRunner()
    result = runner.invoke(channels, ["delete", AN_IDENTIFIER, "--yes"], obj=_ctx())

    assert result.exit_code == 0, result.output
    delete_calls = [c for c in responses.calls if c.request.method == "DELETE"]
    assert len(delete_calls) == 1
    assert delete_calls[0].request.url == f"{CHANNEL_URL}/{AN_IDENTIFIER}"
    assert "DELETED" in result.output


@mock_aws
@responses.activate
def test_delete_prompts_for_confirmation_when_yes_not_set():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, f"{SERIAL_URL}/{AN_IDENTIFIER}", json=_a_serial_hit())

    runner = CliRunner()
    result = runner.invoke(channels, ["delete", AN_IDENTIFIER], input="n\n", obj=_ctx())

    assert result.exit_code != 0
    delete_calls = [c for c in responses.calls if c.request.method == "DELETE"]
    assert len(delete_calls) == 0


@mock_aws
@responses.activate
def test_delete_raises_when_channel_not_found():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, f"{SERIAL_URL}/{AN_IDENTIFIER}", status=404)
    responses.add(responses.GET, f"{PUBLISHER_URL}/{AN_IDENTIFIER}", status=404)

    runner = CliRunner()
    result = runner.invoke(channels, ["delete", AN_IDENTIFIER, "--yes"], obj=_ctx())

    assert result.exit_code != 0
    assert "no channel" in result.output.lower()


@mock_aws
@responses.activate
def test_search_passes_year_query_param():
    _seed_aws()
    _add_cognito()
    responses.add(responses.GET, SERIAL_URL, json={"hits": [], "totalHits": 0})
    responses.add(responses.GET, PUBLISHER_URL, json={"hits": [], "totalHits": 0})

    runner = CliRunner()
    result = runner.invoke(channels, ["search", "Nature", "--year", "2024"], obj=_ctx())

    assert result.exit_code == 0, result.output
    get_calls = [c for c in responses.calls if c.request.method == "GET"]
    assert len(get_calls) == 2
    for call in get_calls:
        assert "year=2024" in call.request.url


@mock_aws
@responses.activate
def test_get_appends_year_path_segment():
    _seed_aws()
    _add_cognito()
    responses.add(
        responses.GET,
        f"{SERIAL_URL}/{AN_IDENTIFIER}/2024",
        json=_a_serial_hit(),
    )

    runner = CliRunner()
    result = runner.invoke(
        channels, ["get", AN_IDENTIFIER, "--year", "2024"], obj=_ctx()
    )

    assert result.exit_code == 0, result.output
    get_calls = [c for c in responses.calls if c.request.method == "GET"]
    assert len(get_calls) == 1
    assert get_calls[0].request.url.endswith(f"/{AN_IDENTIFIER}/2024")


def test_identifier_from_id_handles_year_suffix():
    channel_id = (
        f"https://{API_DOMAIN}/publication-channels-v2/"
        f"serial-publication/{AN_IDENTIFIER}/2024"
    )
    assert _identifier_from_id(channel_id) == AN_IDENTIFIER


def test_identifier_from_id_handles_no_year_suffix():
    channel_id = (
        f"https://{API_DOMAIN}/publication-channels-v2/publisher/{AN_IDENTIFIER}"
    )
    assert _identifier_from_id(channel_id) == AN_IDENTIFIER


def test_identifier_from_id_handles_journal_kind():
    channel_id = (
        f"https://{API_DOMAIN}/publication-channels-v2/journal/{AN_IDENTIFIER}/2024"
    )
    assert _identifier_from_id(channel_id) == AN_IDENTIFIER


def test_identifier_from_id_empty_string():
    assert _identifier_from_id("") == ""
