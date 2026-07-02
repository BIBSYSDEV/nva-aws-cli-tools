import glob
import json
import os
import urllib.parse

import boto3
import responses
from click.testing import CliRunner
from moto import mock_aws

from commands.search import search, _JsonlSink, _format_hit_line, _split_csv
from commands.utils import AppContext

API_DOMAIN = "api.example.org"
SEARCH_URL = f"https://{API_DOMAIN}/search/resources"
A_PROFILE = "testprofile"
A_UNIT = "194.0.0.0"


def _ctx() -> AppContext:
    return AppContext(
        log_level=0,
        profile=A_PROFILE,
        session=boto3.Session(region_name="eu-west-1"),
    )


def _seed_ssm() -> None:
    ssm = boto3.client("ssm", region_name="eu-west-1")
    ssm.put_parameter(Name="/NVA/ApiDomain", Value=API_DOMAIN, Type="String")


def _a_hit(identifier: str) -> dict:
    return {"identifier": identifier, "type": "Publication"}


def test_format_hit_line_id_only_returns_identifier():
    assert _format_hit_line(_a_hit("abc"), id_only=True, compact=True) == "abc"


def test_format_hit_line_id_only_missing_identifier_returns_none():
    assert _format_hit_line({}, id_only=True, compact=True) is None


def test_format_hit_line_compact_is_single_line_json():
    line = _format_hit_line({"identifier": "abc", "x": 1}, id_only=False, compact=True)

    assert "\n" not in line
    assert json.loads(line) == {"identifier": "abc", "x": 1}


def test_format_hit_line_pretty_is_indented():
    line = _format_hit_line({"identifier": "abc"}, id_only=False, compact=False)

    assert "\n" in line


def test_jsonl_sink_rotates_into_batches(tmp_path):
    base = str(tmp_path / "resultat.jsonl")

    with _JsonlSink(base, batch_size=1000) as sink:
        for index in range(2500):
            sink.write(_format_hit_line(_a_hit(f"id-{index}"), False, True))

    files = sorted(
        os.path.basename(path) for path in glob.glob(str(tmp_path / "*.jsonl"))
    )
    assert files == [
        "resultat_00001.jsonl",
        "resultat_00002.jsonl",
        "resultat_00003.jsonl",
    ]
    assert sink.file_count == 3
    assert [os.path.basename(path) for path in sink.paths] == files

    line_counts = []
    for name in files:
        with open(tmp_path / name, encoding="utf-8") as batch_file:
            line_counts.append(sum(1 for _ in batch_file))
    assert line_counts == [1000, 1000, 500]


def test_jsonl_sink_creates_missing_parent_directories(tmp_path):
    base = str(tmp_path / "nested" / "dir" / "resultat.jsonl")

    with _JsonlSink(base, batch_size=1000) as sink:
        sink.write('{"identifier": "a"}')

    assert os.path.isfile(tmp_path / "nested" / "dir" / "resultat_00001.jsonl")
    assert sink.file_count == 1


def test_jsonl_sink_stdout_mode_prints_and_writes_no_files(capsys):
    with _JsonlSink(None, batch_size=1000) as sink:
        sink.write('{"identifier": "a"}')

    assert sink.file_count == 0
    assert capsys.readouterr().out.strip() == '{"identifier": "a"}'


@mock_aws
@responses.activate
def test_command_writes_batched_jsonl_files():
    _seed_ssm()
    responses.add(
        responses.GET,
        SEARCH_URL,
        json={"hits": [_a_hit("a"), _a_hit("b"), _a_hit("c")]},
    )

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            search,
            [
                "resources",
                "--unit",
                A_UNIT,
                "--output",
                "out.jsonl",
                "--batch-size",
                "2",
            ],
            obj=_ctx(),
        )

        assert result.exit_code == 0, result.output
        assert sorted(glob.glob("*.jsonl")) == ["out_00001.jsonl", "out_00002.jsonl"]

        first_batch = _read_lines("out_00001.jsonl")
        second_batch = _read_lines("out_00002.jsonl")
        assert len(first_batch) == 2
        assert len(second_batch) == 1
        assert json.loads(first_batch[0])["identifier"] == "a"
        assert json.loads(second_batch[0])["identifier"] == "c"


@mock_aws
@responses.activate
def test_exclude_fields_sets_nodes_excluded_query_param():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit("a")]})

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            search,
            [
                "resources",
                "--unit",
                A_UNIT,
                "--exclude-fields",
                "contributorsPreview,tags",
                "--exclude-fields",
                "otherIdentifiers",
                "--output",
                "out.jsonl",
            ],
            obj=_ctx(),
        )

    assert result.exit_code == 0, result.output
    query = urllib.parse.urlparse(responses.calls[0].request.url).query
    params = dict(urllib.parse.parse_qsl(query))
    assert params["nodesExcluded"] == "contributorsPreview,tags,otherIdentifiers"


def test_split_csv_flattens_trims_and_drops_empty():
    assert _split_csv(("a,b", " c ", "", "d")) == ["a", "b", "c", "d"]
    assert _split_csv(()) == []


@mock_aws
@responses.activate
def test_command_uses_suffixed_filename_by_default():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit("a")]})

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            search,
            ["resources", "--unit", A_UNIT, "--output", "out.jsonl"],
            obj=_ctx(),
        )

        assert result.exit_code == 0, result.output
        assert glob.glob("*.jsonl") == ["out_00001.jsonl"]


@mock_aws
@responses.activate
def test_command_creates_output_directory_when_missing():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit("a")]})

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            search,
            ["resources", "--unit", A_UNIT, "--output", "exports/data/out.jsonl"],
            obj=_ctx(),
        )

        assert result.exit_code == 0, result.output
        assert os.path.isfile("exports/data/out_00001.jsonl")


@mock_aws
@responses.activate
def test_command_id_only_prints_identifiers_to_stdout():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit("a"), _a_hit("b")]})

    runner = CliRunner()
    result = runner.invoke(
        search, ["resources", "--unit", A_UNIT, "--id-only"], obj=_ctx()
    )

    assert result.exit_code == 0, result.output
    output_lines = [line for line in result.output.splitlines() if line.strip()]
    assert "a" in output_lines
    assert "b" in output_lines


@mock_aws
@responses.activate
def test_command_default_prints_pretty_json_to_stdout():
    _seed_ssm()
    responses.add(responses.GET, SEARCH_URL, json={"hits": [_a_hit("a")]})

    runner = CliRunner()
    result = runner.invoke(search, ["resources", "--unit", A_UNIT], obj=_ctx())

    assert result.exit_code == 0, result.output
    assert '"identifier": "a"' in result.output
    assert "{\n" in result.output


def _read_lines(filename: str) -> list:
    with open(filename, encoding="utf-8") as file:
        return file.read().splitlines()
