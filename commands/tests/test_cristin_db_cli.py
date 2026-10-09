from unittest.mock import MagicMock

from click.testing import CliRunner

from cli import cli
from commands.services.cristin_db import (
    CristinDatabaseError,
    MergeResult,
    PersonProfile,
)

FROM_LOPENR = 123456
TO_LOPENR = 654321
SESSION_ID = 4711

LOSING_PROFILE = PersonProfile(
    lopenr=FROM_LOPENR,
    attributes={"PERSONLOPENR": FROM_LOPENR, "FORNAVN": "Ola", "ETTERNAVN": "Nordmann"},
    related_counts={"Ansettelser": 2},
)
KEEPING_PROFILE = PersonProfile(
    lopenr=TO_LOPENR,
    attributes={"PERSONLOPENR": TO_LOPENR, "FORNAVN": "Ola", "ETTERNAVN": "Nordmann"},
    related_counts={"Ansettelser": 5},
)


def build_service(monkeypatch, profiles=None, merge_error=None) -> MagicMock:
    service = MagicMock()
    service.environment = "TEST"
    service.dsn = "cmanora-prod05.uio.no:5434/CRISTST.uio.no"
    service.__enter__.return_value = service
    service.__exit__.return_value = False
    lookup = (
        profiles
        if profiles is not None
        else {
            FROM_LOPENR: LOSING_PROFILE,
            TO_LOPENR: KEEPING_PROFILE,
        }
    )
    service.fetch_person.side_effect = lambda lopenr: lookup.get(lopenr)
    if merge_error:
        service.merge_person.side_effect = CristinDatabaseError(merge_error)
    else:
        service.merge_person.return_value = MergeResult(
            session_id=SESSION_ID, output_lines=["Sessionid: 4711"]
        )
    service.construction = []

    def record_construction(*args, **kwargs):
        service.construction.append((args, kwargs))
        return service

    monkeypatch.setattr(
        "commands.cristin_db.CristinDatabaseService", record_construction
    )
    return service


def test_merge_person_shows_preview_and_merges_after_confirmation(monkeypatch):
    service = build_service(monkeypatch)

    result = CliRunner().invoke(
        cli,
        ["cristin-db", "merge-person", str(FROM_LOPENR), str(TO_LOPENR)],
        input="y\n",
    )

    assert result.exit_code == 0
    assert "Forsvinner" in result.output
    assert "Ansettelser" in result.output
    assert "Sessionid: 4711" in result.output
    service.merge_person.assert_called_once_with(FROM_LOPENR, TO_LOPENR)


def test_merge_person_builds_the_service_from_profile_and_vault_path(monkeypatch):
    service = build_service(monkeypatch)

    result = CliRunner().invoke(
        cli,
        [
            "--profile",
            "sikt-nva-prod",
            "cristin-db",
            "merge-person",
            str(FROM_LOPENR),
            str(TO_LOPENR),
            "--vault-path",
            "service/cristin/database/prod",
            "--yes",
        ],
    )

    assert result.exit_code == 0
    arguments, keyword_arguments = service.construction[0]
    assert arguments[0] == "sikt-nva-prod"
    assert keyword_arguments["vault_path"] == "service/cristin/database/prod"


def test_merge_person_aborts_when_not_confirmed(monkeypatch):
    service = build_service(monkeypatch)

    result = CliRunner().invoke(
        cli,
        ["cristin-db", "merge-person", str(FROM_LOPENR), str(TO_LOPENR)],
        input="n\n",
    )

    assert result.exit_code != 0
    service.merge_person.assert_not_called()


def test_merge_person_skips_prompt_with_yes(monkeypatch):
    service = build_service(monkeypatch)

    result = CliRunner().invoke(
        cli, ["cristin-db", "merge-person", str(FROM_LOPENR), str(TO_LOPENR), "--yes"]
    )

    assert result.exit_code == 0
    service.merge_person.assert_called_once_with(FROM_LOPENR, TO_LOPENR)


def test_merge_person_reports_unknown_person(monkeypatch):
    service = build_service(monkeypatch, profiles={TO_LOPENR: KEEPING_PROFILE})

    result = CliRunner().invoke(
        cli, ["cristin-db", "merge-person", str(FROM_LOPENR), str(TO_LOPENR), "--yes"]
    )

    assert result.exit_code != 0
    assert f"Fant ingen person med PERSONLOPENR {FROM_LOPENR}" in result.output
    service.merge_person.assert_not_called()


def test_merge_person_reports_database_error(monkeypatch):
    build_service(monkeypatch, merge_error="ORA-20001: noe gikk galt")

    result = CliRunner().invoke(
        cli, ["cristin-db", "merge-person", str(FROM_LOPENR), str(TO_LOPENR), "--yes"]
    )

    assert result.exit_code != 0
    assert "ORA-20001" in result.output
