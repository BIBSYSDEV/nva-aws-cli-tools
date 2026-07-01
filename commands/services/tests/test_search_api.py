import urllib.parse

import boto3
import responses
from moto import mock_aws

from commands.services.search_api import SearchApiService

API_DOMAIN = "api.example.org"
SEARCH_URL = f"https://{API_DOMAIN}/search/resources"
NEXT_URL = f"{SEARCH_URL}?searchAfter=next-cursor&results=2"


def _seed_ssm() -> None:
    ssm = boto3.client("ssm", region_name="eu-west-1")
    ssm.put_parameter(Name="/NVA/ApiDomain", Value=API_DOMAIN, Type="String")


def _a_service() -> SearchApiService:
    return SearchApiService(boto3.Session(region_name="eu-west-1"))


def _a_hit(identifier: str) -> dict:
    return {"identifier": identifier, "type": "Publication"}


def _query_params(call) -> dict:
    query = urllib.parse.urlparse(call.request.url).query
    return dict(urllib.parse.parse_qsl(query))


@mock_aws
@responses.activate
def test_single_page_without_next_link_yields_all_hits():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit("a"), _a_hit("b")]})

    hits = list(_a_service().resource_search({"aggregation": "none"}, page_size=50))

    assert [hit["identifier"] for hit in hits] == ["a", "b"]
    assert len(responses.calls) == 1


@mock_aws
@responses.activate
def test_first_request_uses_results_and_no_from_offset():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit("a")]})

    list(_a_service().resource_search({"aggregation": "none"}, page_size=25))

    params = _query_params(responses.calls[0])
    assert params["results"] == "25"
    assert params["aggregation"] == "none"
    assert "from" not in params


@mock_aws
@responses.activate
def test_follows_next_search_after_link_across_pages():
    _seed_ssm()
    responses.add(
        responses.GET,
        SEARCH_URL,
        json={"hits": [_a_hit("a"), _a_hit("b")], "nextSearchAfterResults": NEXT_URL},
    )
    responses.add(responses.GET, NEXT_URL, json={"hits": [_a_hit("c")]})

    hits = list(_a_service().resource_search({"aggregation": "none"}, page_size=2))

    assert [hit["identifier"] for hit in hits] == ["a", "b", "c"]
    assert len(responses.calls) == 2
    assert "searchAfter=next-cursor" in responses.calls[1].request.url


@mock_aws
@responses.activate
def test_next_page_request_follows_link_verbatim_with_accept_header():
    _seed_ssm()
    responses.add(
        responses.GET,
        SEARCH_URL,
        json={"hits": [_a_hit("a")], "nextSearchAfterResults": NEXT_URL},
    )
    responses.add(responses.GET, NEXT_URL, json={"hits": []})

    list(
        _a_service().resource_search(
            {"aggregation": "none"}, page_size=2, api_version="2099-01-01"
        )
    )

    second_request = responses.calls[1].request
    assert second_request.url == NEXT_URL
    assert "version=2099-01-01" in second_request.headers["Accept"]


@mock_aws
@responses.activate
def test_stops_when_next_link_missing_even_with_hits():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit("a")]})

    hits = list(_a_service().resource_search({"aggregation": "none"}, page_size=1))

    assert [hit["identifier"] for hit in hits] == ["a"]
    assert len(responses.calls) == 1


@mock_aws
@responses.activate
def test_empty_first_page_yields_nothing():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"hits": []})

    hits = list(_a_service().resource_search({"aggregation": "none"}))

    assert hits == []
    assert len(responses.calls) == 1


@mock_aws
@responses.activate
def test_reports_total_hits_once_from_first_page():
    _seed_ssm()
    responses.add(
        responses.GET,
        SEARCH_URL,
        json={
            "hits": [_a_hit("a"), _a_hit("b")],
            "totalHits": 42,
            "nextSearchAfterResults": NEXT_URL,
        },
    )
    responses.add(
        responses.GET, NEXT_URL, json={"hits": [_a_hit("c")], "totalHits": 42}
    )

    reported = []
    list(
        _a_service().resource_search(
            {"aggregation": "none"}, page_size=2, on_total_hits=reported.append
        )
    )

    assert reported == [42]


@mock_aws
@responses.activate
def test_client_error_stops_pagination():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"detail": "bad request"}, status=400)

    hits = list(_a_service().resource_search({"aggregation": "none"}))

    assert hits == []
    assert len(responses.calls) == 1


@mock_aws
@responses.activate
def test_persistent_server_error_gives_up_and_yields_nothing(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda *args, **kwargs: None)
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={}, status=500)

    hits = list(_a_service().resource_search({"aggregation": "none"}))

    assert hits == []
    assert len(responses.calls) == 5
