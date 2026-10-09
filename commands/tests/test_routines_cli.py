from unittest.mock import MagicMock

from click.testing import CliRunner

from cli import cli

FROM_LOPENR = 123456
TO_LOPENR = 654321


def patch_steps(monkeypatch) -> tuple[MagicMock, MagicMock]:
    nva_step = MagicMock()
    cristin_step = MagicMock()
    monkeypatch.setattr("commands.routines.contributor_identifier.callback", nva_step)
    monkeypatch.setattr("commands.routines.run_merge_person", cristin_step)
    return nva_step, cristin_step


def test_merge_person_runs_nva_step_before_cristin_step(monkeypatch):
    nva_step, cristin_step = patch_steps(monkeypatch)

    result = CliRunner().invoke(
        cli, ["routines", "merge-person", str(FROM_LOPENR), str(TO_LOPENR), "--yes"]
    )

    assert result.exit_code == 0
    nva_step.assert_called_once()
    nva_arguments = nva_step.call_args.kwargs
    assert nva_arguments["old_value"] == str(FROM_LOPENR)
    assert nva_arguments["new_value"] == str(TO_LOPENR)
    assert nva_arguments["dry_run_only"] is False
    cristin_step.assert_called_once()
    assert cristin_step.call_args.args[1:4] == (FROM_LOPENR, TO_LOPENR, True)


def test_merge_person_passes_limit_to_nva_step(monkeypatch):
    nva_step, _ = patch_steps(monkeypatch)

    result = CliRunner().invoke(
        cli,
        [
            "routines",
            "merge-person",
            str(FROM_LOPENR),
            str(TO_LOPENR),
            "--limit",
            "50",
            "--yes",
        ],
    )

    assert result.exit_code == 0
    assert nva_step.call_args.kwargs["limit"] == 50


def test_merge_person_stops_when_nva_step_fails(monkeypatch):
    nva_step, cristin_step = patch_steps(monkeypatch)
    nva_step.side_effect = SystemExit(1)

    result = CliRunner().invoke(
        cli, ["routines", "merge-person", str(FROM_LOPENR), str(TO_LOPENR), "--yes"]
    )

    assert result.exit_code != 0
    cristin_step.assert_not_called()
