import polars as pl

from commands.services.report_compare import diff_dataframes, diff_json


def _author_shares_frame(points: list[float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "NVAID": ["https://api.nva.unit.no/publication/abc", None, None],
            "PERSONLOPENR": ["1", None, None],
            "INSTITUSJON": ["1965.1.0.0", "2012.9.20.0", "184.12.24.0"],
            "TITTEL": ["Paper A", "Paper B", "Paper C"],
            "ETTERNAVN": ["Hansen", "Olsen", "Lipniacka"],
            "FORNAVN": ["Per", "Kari", "Anna"],
            "PUBLISERINGSPOENG": points,
        }
    )


def test_identical_frames_report_no_differences():
    frame = _author_shares_frame([0.1667, 0.169, 0.0226])

    result = diff_dataframes(frame, frame)

    assert result["changed"] == []
    assert result["removed"].is_empty()
    assert result["added"].is_empty()
    assert result["ambiguous_removed"].is_empty()
    assert result["ambiguous_added"].is_empty()
    assert result["numeric_totals"] == []


def test_only_manipulated_cell_is_reported_as_changed():
    baseline = _author_shares_frame([0.1667, 0.169, 0.0226])
    current = _author_shares_frame([2.1667, 0.169, 0.0226])

    result = diff_dataframes(baseline, current)

    assert len(result["changed"]) == 1
    change = result["changed"][0]
    assert change["column"] == "PUBLISERINGSPOENG"
    assert change["key"]["TITTEL"] == "Paper A"
    assert change["baseline"] == 0.1667
    assert change["current"] == 2.1667
    assert result["removed"].is_empty()
    assert result["added"].is_empty()


def test_external_author_without_ids_is_keyed_by_name_and_title():
    baseline = _author_shares_frame([0.1667, 0.169, 0.0226])
    current = _author_shares_frame([0.1667, 0.169, 9.9])

    result = diff_dataframes(baseline, current)

    assert len(result["changed"]) == 1
    assert result["changed"][0]["key"]["ETTERNAVN"] == "Lipniacka"
    assert result["changed"][0]["current"] == 9.9


def test_reordered_rows_report_no_differences():
    baseline = _author_shares_frame([0.1667, 0.169, 0.0226])

    result = diff_dataframes(baseline, baseline.reverse())

    assert result["changed"] == []
    assert result["removed"].is_empty()
    assert result["added"].is_empty()


def test_differing_url_host_does_not_flag_rows():
    baseline = _author_shares_frame([0.1667, 0.169, 0.0226])
    current = baseline.with_columns(
        pl.col("NVAID").str.replace(
            "https://api.nva.unit.no", "https://api.test.nva.aws.unit.no"
        )
    )

    result = diff_dataframes(baseline, current)

    assert result["changed"] == []
    assert result["removed"].is_empty()


def test_xlsx_roundtrip_float_noise_is_not_reported():
    baseline = _author_shares_frame([0.16666666666666666, 0.169, 0.0226])
    current = _author_shares_frame([0.1666666666666667, 0.169, 0.0226])

    result = diff_dataframes(baseline, current)

    assert result["changed"] == []
    assert result["numeric_totals"] == []


def test_identical_duplicate_rows_are_not_flagged():
    baseline = _author_shares_frame([0.1667, 0.169, 0.0226])
    duplicated = pl.concat([baseline, baseline.tail(1)])

    result = diff_dataframes(duplicated, duplicated)

    assert result["changed"] == []
    assert result["ambiguous_removed"].is_empty()
    assert result["ambiguous_added"].is_empty()


def test_ambiguous_key_with_changed_count_is_listed_separately():
    baseline = _author_shares_frame([0.1667, 0.169, 0.0226])
    baseline = pl.concat([baseline, baseline.tail(1)])
    current = baseline.head(3)

    result = diff_dataframes(baseline, current)

    assert result["changed"] == []
    assert result["removed"].is_empty()
    assert not result["ambiguous_removed"].is_empty()
    assert result["ambiguous_removed"]["baseline_count"].to_list() == [2]
    assert result["ambiguous_removed"]["current_count"].to_list() == [1]


def test_json_diff_ignores_environment_host():
    baseline = {"id": "https://api.test.example.org/x", "points": 1.0}
    current = {"id": "https://api.prod.example.org/x", "points": 1.0}

    assert diff_json(baseline, current) == []
