from pathlib import Path

import log_config


def test_json_log_file_is_next_to_cli_regardless_of_working_directory(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)

    json_handler = log_config.get_json_handler()
    json_handler.close()

    log_file = Path(json_handler.baseFilename)
    assert log_file.parent == Path(log_config.__file__).resolve().parent
    assert log_file.name == "logs.jsonl"
    assert not (tmp_path / "logs.jsonl").exists()
