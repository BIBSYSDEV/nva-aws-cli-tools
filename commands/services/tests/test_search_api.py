import urllib.parse

import boto3
import requests
import responses
from moto import mock_aws
from responses import matchers

from commands.services.search_api import (
    AdaptivePageSize,
    SearchApiService,
    _accept_header,
    _search_after_cursor,
    _url_with_page_size,
)

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
def test_sends_identifying_user_agent():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit("a")]})

    list(_a_service().resource_search({"aggregation": "none"}))

    assert "nva-aws-cli-tools" in responses.calls[0].request.headers["User-Agent"]


@mock_aws
@responses.activate
def test_empty_api_version_omits_version_from_accept_header():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit("a")]})

    list(_a_service().resource_search({"aggregation": "none"}, api_version=""))

    assert responses.calls[0].request.headers["Accept"] == "application/json"


def test_accept_header_includes_version_when_set():
    assert _accept_header("2024-12-01") == "application/json; version=2024-12-01"


def test_accept_header_omits_version_when_empty():
    assert _accept_header("") == "application/json"
    assert _accept_header(None) == "application/json"


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
def test_next_page_keeps_cursor_and_sets_current_page_size():
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
    params = _query_params(responses.calls[1])
    assert params["searchAfter"] == "next-cursor"
    assert params["results"] == "2"
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
def test_invalid_json_body_yields_nothing_without_crashing():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, body="<html>not json</html>", status=200)

    hits = list(_a_service().resource_search({"aggregation": "none"}))

    assert hits == []
    assert len(responses.calls) == 1


@mock_aws
@responses.activate
def test_persistent_server_error_backs_off_to_minimum_then_gives_up(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda *args, **kwargs: None)
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={}, status=500)

    hits = list(_a_service().resource_search({"aggregation": "none"}, page_size=2))

    assert hits == []
    sizes_requested = {_query_params(call)["results"] for call in responses.calls}
    assert sizes_requested == {"2", "1"}


@mock_aws
@responses.activate
def test_server_error_is_not_retried_before_reducing_page_size(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda *args, **kwargs: None)
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={}, status=500)

    list(_a_service().resource_search({"aggregation": "none"}, page_size=2))

    assert len(responses.calls) == 2


@mock_aws
@responses.activate
def test_network_errors_are_retried_by_count_without_page_size_backoff(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda *args, **kwargs: None)
    _seed_ssm()
    responses.add(
        responses.GET, SEARCH_URL, body=requests.exceptions.ConnectionError("boom")
    )

    hits = list(_a_service().resource_search({"aggregation": "none"}, page_size=5))

    assert hits == []
    assert len(responses.calls) == 3
    sizes_requested = {_query_params(call)["results"] for call in responses.calls}
    assert sizes_requested == {"5"}


@mock_aws
@responses.activate
def test_network_error_is_terminal_without_poison_diagnostics(monkeypatch, caplog):
    monkeypatch.setattr("time.sleep", lambda *args, **kwargs: None)
    _seed_ssm()
    responses.add(
        responses.GET, SEARCH_URL, body=requests.exceptions.ConnectionError("boom")
    )

    with caplog.at_level("ERROR"):
        list(_a_service().resource_search({"aggregation": "none"}, page_size=5))

    assert "poisoned record" not in caplog.text
    assert "network error" in caplog.text


@mock_aws
@responses.activate
def test_page_size_params_from_query_do_not_override_adaptive_control():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit("a")]})

    list(
        _a_service().resource_search(
            {"aggregation": "none", "results": "999", "size": "999"}, page_size=25
        )
    )

    params = _query_params(responses.calls[0])
    assert params["results"] == "25"
    assert "size" not in params


@mock_aws
@responses.activate
def test_server_error_reduces_page_size_and_recovers(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda *args, **kwargs: None)
    _seed_ssm()
    responses.add(
        responses.GET,
        SEARCH_URL,
        match=[matchers.query_param_matcher({"results": "2"}, strict_match=False)],
        json={},
        status=500,
    )
    responses.add(
        responses.GET,
        SEARCH_URL,
        match=[matchers.query_param_matcher({"results": "1"}, strict_match=False)],
        json={"hits": [_a_hit("a")]},
    )

    hits = list(_a_service().resource_search({"aggregation": "none"}, page_size=2))

    assert [hit["identifier"] for hit in hits] == ["a"]


@mock_aws
@responses.activate
def test_poison_diagnostics_logged_at_minimum_page_size(monkeypatch, caplog):
    monkeypatch.setattr("time.sleep", lambda *args, **kwargs: None)
    _seed_ssm()
    poison_next = f"{SEARCH_URL}?search_after=poison-cursor&size=1"
    responses.add(
        responses.GET,
        SEARCH_URL,
        match=[
            matchers.query_param_matcher({"aggregation": "none"}, strict_match=False)
        ],
        json={"hits": [_a_hit("last-good")], "nextSearchAfterResults": poison_next},
    )
    responses.add(
        responses.GET,
        SEARCH_URL,
        match=[
            matchers.query_param_matcher(
                {"search_after": "poison-cursor"}, strict_match=False
            )
        ],
        json={},
        status=500,
    )

    with caplog.at_level("ERROR"):
        hits = list(_a_service().resource_search({"aggregation": "none"}, page_size=1))

    assert [hit["identifier"] for hit in hits] == ["last-good"]
    assert "poisoned record" in caplog.text
    assert "poison-cursor" in caplog.text
    assert "last-good" in caplog.text


@mock_aws
@responses.activate
def test_client_error_does_not_trigger_page_size_backoff(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda *args, **kwargs: None)
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"detail": "bad request"}, status=400)

    hits = list(_a_service().resource_search({"aggregation": "none"}, page_size=100))

    assert hits == []
    assert len(responses.calls) == 1


def test_adaptive_page_size_ramps_up_additively_after_successes():
    control = AdaptivePageSize(
        maximum=500, start=50, step=50, successes_before_growth=2
    )
    assert control.current == 50
    control.register_success()
    assert control.current == 50
    control.register_success()
    assert control.current == 100


def test_adaptive_page_size_halves_on_shrink_down_to_minimum():
    control = AdaptivePageSize(maximum=64, start=64, minimum=1)
    sizes = []
    while control.can_shrink():
        sizes.append(control.shrink())
    assert sizes == [32, 16, 8, 4, 2, 1]
    assert control.can_shrink() is False


def test_adaptive_page_size_never_grows_past_maximum():
    control = AdaptivePageSize(maximum=60, start=50, step=50, successes_before_growth=1)
    control.register_success()
    assert control.current == 60


def test_adaptive_page_size_clamps_start_within_bounds():
    control = AdaptivePageSize(maximum=10, start=50)
    assert control.current == 10


def test_shrink_resets_success_streak():
    control = AdaptivePageSize(
        maximum=500, start=50, step=50, successes_before_growth=2
    )
    control.register_success()
    control.shrink()
    control.register_success()
    assert control.current == 25


def test_url_with_page_size_overrides_both_size_and_results():
    url = f"{SEARCH_URL}?search_after=abc%2Cdef&size=25&aggregation=none"

    params = _query_params_from_url(_url_with_page_size(url, 5))

    assert params["size"] == "5"
    assert params["results"] == "5"
    assert params["search_after"] == "abc,def"


def test_url_with_page_size_adds_results_when_no_size_present():
    params = _query_params_from_url(_url_with_page_size(f"{SEARCH_URL}?a=1", 3))

    assert params["results"] == "3"
    assert "size" not in params


def test_search_after_cursor_reads_snake_case_and_camel_case():
    assert _search_after_cursor(f"{SEARCH_URL}?search_after=snake") == "snake"
    assert _search_after_cursor(f"{SEARCH_URL}?searchAfter=camel") == "camel"
    assert _search_after_cursor(f"{SEARCH_URL}?other=1") is None


def _query_params_from_url(url: str) -> dict:
    query = urllib.parse.urlparse(url).query
    return dict(urllib.parse.parse_qsl(query))
