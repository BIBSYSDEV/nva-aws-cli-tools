import boto3
import pytest
import responses
from moto import mock_aws

from commands.services.entity_lookup import EntityResolver, _display_name

API_DOMAIN = "api.example.org"
PUBLISHER_URL = f"https://{API_DOMAIN}/publication-channels-v2/publisher"
PERSON_URL = f"https://{API_DOMAIN}/cristin/person"
PROJECT_URL = f"https://{API_DOMAIN}/cristin/project"


def _seed_aws() -> None:
    ssm = boto3.client("ssm", region_name="eu-west-1")
    ssm.put_parameter(Name="/NVA/ApiDomain", Value=API_DOMAIN, Type="String")
    ssm.put_parameter(
        Name="/NVA/CognitoUri", Value="https://cognito.example.org", Type="String"
    )


def _resolver() -> EntityResolver:
    return EntityResolver(session=boto3.Session(region_name="eu-west-1"))


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ({"name": "Universitetsforlaget"}, "Universitetsforlaget"),
        ({"title": "A research project"}, "A research project"),
        ({"title": {"nb": "Tittel", "en": "Title"}}, "Tittel"),
        ({"labels": {"en": "Department", "nb": "Institutt"}}, "Institutt"),
        ({"labels": {"en": "Department only"}}, "Department only"),
        (
            {
                "names": [
                    {"type": "FirstName", "value": "Ola"},
                    {"type": "LastName", "value": "Nordmann"},
                ]
            },
            "Ola Nordmann",
        ),
        ({"firstName": "Kari", "lastName": "Nordmann"}, "Kari Nordmann"),
        ({"first_name": "Kari", "surname": "Nordmann"}, "Kari Nordmann"),
        ({"unknown": "value"}, None),
        ({}, None),
        (None, None),
    ],
)
def test_display_name_handles_known_shapes(data, expected):
    assert _display_name(data) == expected


@mock_aws
@responses.activate
def test_publisher_name_reads_channel_name():
    _seed_aws()
    responses.add(
        responses.GET, f"{PUBLISHER_URL}/PUB-1", json={"name": "Cappelen Damm"}
    )

    assert _resolver().publisher_name("PUB-1") == "Cappelen Damm"


@mock_aws
@responses.activate
def test_person_name_reads_from_cristin_proxy():
    _seed_aws()
    responses.add(
        responses.GET,
        f"{PERSON_URL}/1366281",
        json={
            "names": [
                {"type": "FirstName", "value": "Ada"},
                {"type": "LastName", "value": "Lovelace"},
            ]
        },
    )

    assert _resolver().person_name("1366281") == "Ada Lovelace"


@mock_aws
@responses.activate
def test_project_title_reads_from_cristin_proxy():
    _seed_aws()
    responses.add(
        responses.GET, f"{PROJECT_URL}/2748467", json={"title": "Deep Sea Mapping"}
    )

    assert _resolver().project_title("2748467") == "Deep Sea Mapping"


@mock_aws
@responses.activate
def test_organization_label_reads_from_uri():
    _seed_aws()
    org_uri = "https://api.example.org/cristin/organization/209.1.0.0"
    responses.add(responses.GET, org_uri, json={"labels": {"nb": "Institutt for IT"}})

    assert _resolver().organization_label(org_uri) == "Institutt for IT"


@mock_aws
@responses.activate
def test_failed_lookup_returns_none():
    _seed_aws()
    responses.add(responses.GET, f"{PUBLISHER_URL}/MISSING", status=404)

    assert _resolver().publisher_name("MISSING") is None


@mock_aws
def test_lookup_returns_none_when_api_domain_unavailable():
    assert _resolver().person_name("1366281") is None
