import boto3
import pytest
import responses
from moto import mock_aws

from commands.services.entity_lookup import (
    EntityResolver,
    _display_label_person,
    _display_name_organization,
    _display_name_person,
    _display_name_project,
    _display_name_publisher,
)

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


def _seed_backend_credentials() -> None:
    secretsmanager = boto3.client("secretsmanager", region_name="eu-west-1")
    secretsmanager.create_secret(
        Name="BackendCognitoClientCredentials",
        SecretString='{"backendClientId": "client-id", "backendClientSecret": "client-secret"}',
    )


def _resolver() -> EntityResolver:
    return EntityResolver(session=boto3.Session(region_name="eu-west-1"))


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ({"name": "Universitetsforlaget"}, "Universitetsforlaget"),
        ({"name": {"nb": "Forlaget", "en": "The Publisher"}}, "Forlaget"),
        ({"unknown": "value"}, None),
        ({}, None),
        (None, None),
    ],
)
def test_display_name_publisher_handles_known_shapes(data, expected):
    assert _display_name_publisher(data) == expected


@pytest.mark.parametrize(
    ("data", "expected"),
    [
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
def test_display_name_person_handles_known_shapes(data, expected):
    assert _display_name_person(data) == expected


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        (
            {
                "names": [
                    {"type": "FirstName", "value": "Ola"},
                    {"type": "LastName", "value": "Nordmann"},
                ],
                "identifiers": [
                    {"type": "CristinIdentifier", "value": "1366281"},
                    {"type": "NationalIdentificationNumber", "value": "01019012345"},
                ],
            },
            "Ola Nordmann, fnr 01019012345",
        ),
        (
            {
                "names": [{"type": "FirstName", "value": "Ola"}],
                "identifiers": [{"type": "CristinIdentifier", "value": "1366281"}],
            },
            "Ola",
        ),
        (
            {"identifiers": [{"type": "NationalIdentificationNumber", "value": "x"}]},
            None,
        ),
        (None, None),
    ],
)
def test_display_label_person_includes_national_id_when_present(data, expected):
    assert _display_label_person(data) == expected


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ({"title": "A research project"}, "A research project"),
        ({"title": {"nb": "Tittel", "en": "Title"}}, "Tittel"),
        ({"unknown": "value"}, None),
        ({}, None),
        (None, None),
    ],
)
def test_display_name_project_handles_known_shapes(data, expected):
    assert _display_name_project(data) == expected


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ({"labels": {"en": "Department", "nb": "Institutt"}}, "Institutt"),
        ({"labels": {"en": "Department only"}}, "Department only"),
        ({"unknown": "value"}, None),
        ({}, None),
        (None, None),
    ],
)
def test_display_name_organization_handles_known_shapes(data, expected):
    assert _display_name_organization(data) == expected


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
def test_person_label_falls_back_to_unauthenticated_lookup():
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

    assert _resolver().person_label("1366281") == "Ada Lovelace"


@mock_aws
@responses.activate
def test_person_label_falls_back_to_raw_position_code_and_org_segment():
    _seed_aws()
    org_uri = f"https://{API_DOMAIN}/cristin/organization/20202.0.0.0"
    responses.add(
        responses.GET,
        f"{PERSON_URL}/1366281",
        json={
            "names": [
                {"type": "FirstName", "value": "Ada"},
                {"type": "LastName", "value": "Lovelace"},
            ],
            "employments": [
                {
                    "type": f"https://{API_DOMAIN}/cristin/position#1087",
                    "organization": org_uri,
                }
            ],
        },
    )
    responses.add(responses.GET, f"https://{API_DOMAIN}/cristin/position", status=404)
    responses.add(responses.GET, org_uri, status=404)

    assert _resolver().person_label("1366281") == ("Ada Lovelace\n  1087, 20202.0.0.0")


@mock_aws
@responses.activate
def test_person_label_uses_auth_token_and_includes_national_id():
    _seed_aws()
    _seed_backend_credentials()
    responses.add(
        responses.POST,
        "https://cognito.example.org/oauth2/token",
        json={"access_token": "test-token", "expires_in": 3600},
    )
    org_uri = f"https://{API_DOMAIN}/cristin/organization/20202.0.0.0"
    responses.add(
        responses.GET,
        f"{PERSON_URL}/1366281",
        json={
            "names": [
                {"type": "FirstName", "value": "Ada"},
                {"type": "LastName", "value": "Lovelace"},
            ],
            "identifiers": [
                {"type": "CristinIdentifier", "value": "1366281"},
                {"type": "NationalIdentificationNumber", "value": "01019012345"},
            ],
            "employments": [
                {
                    "type": f"https://{API_DOMAIN}/cristin/position#1087",
                    "organization": org_uri,
                    "startDate": "2008-01-01T00:00:00Z",
                    "endDate": "2019-12-31T00:00:00Z",
                    "fullTimeEquivalentPercentage": 100.0,
                }
            ],
        },
        match=[
            responses.matchers.header_matcher({"Authorization": "Bearer test-token"})
        ],
    )
    responses.add(
        responses.GET,
        f"https://{API_DOMAIN}/cristin/position",
        json={
            "positions": [
                {
                    "id": f"https://{API_DOMAIN}/positions#1087",
                    "labels": {"nb": "Overingeniør"},
                }
            ]
        },
    )
    responses.add(responses.GET, org_uri, json={"labels": {"nb": "Institutt for IT"}})

    assert _resolver().person_label("1366281") == (
        "Ada Lovelace, fnr 01019012345\n"
        "  Overingeniør, Institutt for IT (2008-01-01–2019-12-31, 100%)"
    )


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
    assert _resolver().person_label("1366281") is None
