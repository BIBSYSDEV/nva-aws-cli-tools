import inspect
from unittest.mock import MagicMock

from click.testing import CliRunner

from cli import cli
from commands.manual_update import contributor_identifier

FROM_LOPENR = 123456
TO_LOPENR = 654321

NVA_STEP = "nva"
CRISTIN_STEP = "cristin"
VALIDATION_STEP = "validation"


def patch_steps(monkeypatch, nva_report=None) -> list[str]:
    calls: list[str] = []
    nva_step = MagicMock(
        side_effect=lambda **kwargs: calls.append(NVA_STEP) or nva_report
    )
    cristin_step = MagicMock(
        side_effect=lambda *args, **kwargs: calls.append(CRISTIN_STEP)
    )
    validation = MagicMock(
        side_effect=lambda *args, **kwargs: calls.append(VALIDATION_STEP)
    )
    monkeypatch.setattr("commands.routines.contributor_identifier.callback", nva_step)
    monkeypatch.setattr("commands.routines.run_merge_person", cristin_step)
    monkeypatch.setattr("commands.routines.require_persons_exist", validation)
    return calls, nva_step, cristin_step, validation


def run_routine(*extra_arguments: str):
    return CliRunner().invoke(
        cli,
        [
            "routines",
            "merge-person",
            str(FROM_LOPENR),
            str(TO_LOPENR),
            *extra_arguments,
        ],
    )


def test_merge_person_validates_then_moves_in_nva_then_merges(monkeypatch):
    calls, nva_step, _, validation = patch_steps(monkeypatch)

    result = run_routine("--yes")

    assert result.exit_code == 0
    assert calls == [VALIDATION_STEP, NVA_STEP, CRISTIN_STEP]
    assert validation.call_args.args[1] == (FROM_LOPENR, TO_LOPENR)
    nva_arguments = nva_step.call_args.kwargs
    assert nva_arguments["old_value"] == str(FROM_LOPENR)
    assert nva_arguments["new_value"] == str(TO_LOPENR)


def test_nva_step_arguments_match_the_real_command_signature(monkeypatch):
    _, nva_step, _, _ = patch_steps(monkeypatch)

    run_routine("--yes")

    signature = inspect.signature(contributor_identifier.callback)
    signature.bind_partial(**nva_step.call_args.kwargs)


def test_merge_person_passes_limit_to_nva_step(monkeypatch):
    _, nva_step, _, _ = patch_steps(monkeypatch)

    result = run_routine("--limit", "50", "--yes")

    assert result.exit_code == 0
    assert nva_step.call_args.kwargs["limit"] == 50


def test_merge_person_stops_when_nva_step_has_results_left(monkeypatch):
    calls, _, cristin_step, _ = patch_steps(
        monkeypatch,
        nva_report={"limitReached": True, "limit": 10, "totalHits": 42, "changes": []},
    )

    result = run_routine("--yes")

    assert result.exit_code != 0
    assert "limitReached" in result.output
    assert CRISTIN_STEP not in calls
    cristin_step.assert_not_called()


def test_merge_person_continues_when_nva_step_moved_everything(monkeypatch):
    calls, _, cristin_step, _ = patch_steps(
        monkeypatch,
        nva_report={"limitReached": False, "totalHits": 3, "changes": [{}, {}, {}]},
    )

    result = run_routine("--yes")

    assert result.exit_code == 0
    assert CRISTIN_STEP in calls
    cristin_step.assert_called_once()


def test_merge_person_stops_when_the_profiles_cannot_be_read(monkeypatch):
    _, nva_step, cristin_step, validation = patch_steps(monkeypatch)
    validation.side_effect = SystemExit(1)

    result = run_routine("--yes")

    assert result.exit_code != 0
    nva_step.assert_not_called()
    cristin_step.assert_not_called()


def test_merge_person_stops_when_nva_step_fails(monkeypatch):
    _, nva_step, cristin_step, _ = patch_steps(monkeypatch)
    nva_step.side_effect = SystemExit(1)

    result = run_routine("--yes")

    assert result.exit_code != 0
    cristin_step.assert_not_called()
