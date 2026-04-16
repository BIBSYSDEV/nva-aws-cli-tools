import base64
import json
import math
import zlib
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest
from boto3.dynamodb.types import Binary

from commands.dynamodb import _parse_filter_expression, _parse_multiple_filters
from commands.services.dynamodb_exporter import DynamoDBEncoder, GenericDynamodbExporter


@pytest.fixture
def mock_session():
    with patch("commands.services.dynamodb_exporter.boto3.Session") as mock:
        yield mock


@pytest.fixture
def mock_exporter(mock_session):
    mock_client = Mock()
    mock_client.list_tables.return_value = {"TableNames": ["test-table"]}

    mock_table = Mock()
    mock_resource = Mock()
    mock_resource.Table.return_value = mock_table

    mock_session_instance = Mock()
    mock_session_instance.client.return_value = mock_client
    mock_session_instance.resource.return_value = mock_resource
    mock_session.return_value = mock_session_instance

    return GenericDynamodbExporter(None, "test-table")


@pytest.fixture
def sample_compressed_item():
    data = {"key": "value", "nested": {"field": "data"}}
    data_str = json.dumps(data)
    compress_obj = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    compressed_data = compress_obj.compress(data_str.encode()) + compress_obj.flush()
    encoded_data = base64.b64encode(compressed_data).decode()

    return {
        "PK0": "Resource:123",
        "SK0": "Resource:123",
        "data": encoded_data,
        "someField": "someValue",
    }


@pytest.fixture
def sample_uncompressed_item():
    return {
        "PK0": "Resource:456",
        "SK0": "Resource:456",
        "someField": "someValue",
        "otherField": "otherValue",
    }


def test_get_table_success(mock_session):
    mock_client = Mock()
    mock_client.list_tables.return_value = {
        "TableNames": ["my-table-dev-123", "other-table", "my-table-prod-456"]
    }

    mock_resource = Mock()
    mock_table = Mock()
    mock_table.name = "my-table-prod-456"
    mock_resource.Table.return_value = mock_table

    mock_session_instance = Mock()
    mock_session_instance.client.return_value = mock_client
    mock_session_instance.resource.return_value = mock_resource
    mock_session.return_value = mock_session_instance

    exporter = GenericDynamodbExporter("test-profile", "prod")

    assert exporter.table is not None
    assert exporter.table_name == "my-table-prod-456"


def test_get_table_not_found(mock_session):
    mock_client = Mock()
    mock_client.list_tables.return_value = {"TableNames": ["other-table", "another-table"]}

    mock_session_instance = Mock()
    mock_session_instance.client.return_value = mock_client
    mock_session.return_value = mock_session_instance

    with pytest.raises(ValueError, match="No table found containing 'resources'"):
        GenericDynamodbExporter("test-profile", "resources")


def test_decompress_data(mock_exporter):
    original_data = {"key": "value", "nested": {"field": "data"}}
    data_str = json.dumps(original_data)
    compress_obj = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    compressed_data = compress_obj.compress(data_str.encode()) + compress_obj.flush()
    encoded_data = base64.b64encode(compressed_data).decode()

    decompressed = mock_exporter._decompress_data(encoded_data)

    assert decompressed == original_data


def test_process_item_with_compression(mock_exporter, sample_compressed_item):
    processed = mock_exporter._process_item(sample_compressed_item)

    assert "@data_decompressed" in processed
    assert processed["@data_decompressed"]["key"] == "value"
    assert processed["PK0"] == "Resource:123"
    assert processed["someField"] == "someValue"


def test_process_item_with_binary_compression(mock_exporter):
    original_data = {"key": "value", "nested": {"field": "data"}}
    data_str = json.dumps(original_data)
    compress_obj = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    compressed_data = compress_obj.compress(data_str.encode()) + compress_obj.flush()
    binary_data = Binary(compressed_data)

    item_with_binary = {
        "PK0": "Resource:789",
        "SK0": "Resource:789",
        "data": binary_data,
        "someField": "someValue",
    }

    processed = mock_exporter._process_item(item_with_binary)

    assert "@data_decompressed" in processed
    assert processed["@data_decompressed"]["key"] == "value"
    assert processed["@data_decompressed"]["nested"]["field"] == "data"
    assert processed["PK0"] == "Resource:789"
    assert processed["someField"] == "someValue"


def test_process_item_without_compression(mock_exporter, sample_uncompressed_item):
    processed = mock_exporter._process_item(sample_uncompressed_item)

    assert "@data_decompressed" not in processed
    assert processed == sample_uncompressed_item


def test_save_items_to_file(mock_exporter, sample_uncompressed_item, tmp_path):
    items = [sample_uncompressed_item]
    mock_exporter._save_items_to_file(items, 1, str(tmp_path))

    output_file = tmp_path / "batch_00001.jsonl"
    assert output_file.exists()

    with open(output_file) as file:
        lines = file.readlines()
        assert len(lines) == 1
        saved_item = json.loads(lines[0])
        assert saved_item == sample_uncompressed_item


def test_save_mixed_compressed_and_uncompressed_items(
    mock_exporter, sample_compressed_item, sample_uncompressed_item, tmp_path
):
    items = [sample_compressed_item, sample_uncompressed_item]
    mock_exporter._save_items_to_file(items, 1, str(tmp_path))

    output_file = tmp_path / "batch_00001.jsonl"
    assert output_file.exists()

    with open(output_file) as file:
        lines = file.readlines()
        assert len(lines) == 2

        compressed_result = json.loads(lines[0])
        assert "@data_decompressed" in compressed_result
        assert compressed_result["@data_decompressed"]["key"] == "value"
        assert compressed_result["PK0"] == "Resource:123"

        uncompressed_result = json.loads(lines[1])
        assert "@data_decompressed" not in uncompressed_result
        assert uncompressed_result["PK0"] == "Resource:456"


def test_parse_filter_expression_exists():
    result = _parse_filter_expression("someField:exists")
    assert result is not None
    assert hasattr(result, "get_expression")


def test_parse_filter_expression_not_exists():
    result = _parse_filter_expression("someField:not_exists")
    assert result is not None
    assert hasattr(result, "get_expression")


def test_parse_filter_expression_begins_with():
    result = _parse_filter_expression("PK0:begins_with:Resource:")
    assert result is not None
    assert hasattr(result, "get_expression")


def test_parse_filter_expression_eq():
    result = _parse_filter_expression("status:eq:active")
    assert result is not None
    assert hasattr(result, "get_expression")


def test_parse_filter_expression_with_colon_in_value():
    result = _parse_filter_expression("url:begins_with:https://example.com")
    assert result is not None
    assert hasattr(result, "get_expression")


def test_parse_filter_expression_invalid_format():
    with pytest.raises(ValueError, match="Filter expression must be in format"):
        _parse_filter_expression("invalid")


def test_parse_filter_expression_unsupported_operator():
    with pytest.raises(ValueError, match="Unsupported operator"):
        _parse_filter_expression("field:invalid_op:value")


def test_parse_multiple_filters_single_filter():
    result = _parse_multiple_filters(("status:eq:PUBLISHED",))
    assert result is not None
    assert hasattr(result, "get_expression")


def test_parse_multiple_filters_two_filters():
    result = _parse_multiple_filters(
        ("status:eq:PUBLISHED", "modifiedDate:gt:2025-01-01")
    )
    assert result is not None
    assert hasattr(result, "get_expression")


def test_parse_multiple_filters_three_filters():
    result = _parse_multiple_filters(
        (
            "PK0:begins_with:Resource",
            "status:eq:PUBLISHED",
            "modifiedDate:gt:2025-01-01",
        )
    )
    assert result is not None
    assert hasattr(result, "get_expression")


def test_parse_multiple_filters_empty_tuple():
    result = _parse_multiple_filters(())
    assert result is None


def test_dynamodb_encoder_decimal_integer():
    data = {"count": Decimal("42")}
    result = json.dumps(data, cls=DynamoDBEncoder)
    assert result == '{"count": 42}'


def test_dynamodb_encoder_decimal_float():
    data = {"price": Decimal("19.99")}
    result = json.dumps(data, cls=DynamoDBEncoder)
    assert result == '{"price": 19.99}'


def test_dynamodb_encoder_binary():
    binary_data = Binary(b"test data")
    data = {"binary_field": binary_data}
    result = json.dumps(data, cls=DynamoDBEncoder)
    expected_b64 = base64.b64encode(b"test data").decode("utf-8")
    assert result == f'{{"binary_field": "{expected_b64}"}}'


def test_dynamodb_encoder_set():
    data = {"tags": {"tag1", "tag2", "tag3"}}
    result = json.dumps(data, cls=DynamoDBEncoder)
    parsed = json.loads(result)
    assert set(parsed["tags"]) == {"tag1", "tag2", "tag3"}


def test_dynamodb_encoder_datetime():
    dt = datetime(2024, 1, 15, 10, 30, 45)
    data = {"timestamp": dt}
    result = json.dumps(data, cls=DynamoDBEncoder)
    assert result == '{"timestamp": "2024-01-15T10:30:45"}'


def test_dynamodb_encoder_date():
    d = date(2024, 1, 15)
    data = {"date_field": d}
    result = json.dumps(data, cls=DynamoDBEncoder)
    assert result == '{"date_field": "2024-01-15"}'


def test_save_items_to_file_with_segment(mock_exporter, sample_uncompressed_item, tmp_path):
    items = [sample_uncompressed_item]
    mock_exporter._save_items_to_file(items, 1, str(tmp_path), segment=2)

    output_file = tmp_path / "segment_002_batch_00001.jsonl"
    assert output_file.exists()

    with open(output_file) as file:
        lines = file.readlines()
        assert len(lines) == 1
        saved_item = json.loads(lines[0])
        assert saved_item == sample_uncompressed_item


def test_export_parallel_scan_creates_segment_files(mock_exporter, sample_uncompressed_item, tmp_path):
    total_segments = 3

    mock_table = Mock()
    mock_table.scan.return_value = {"Items": [sample_uncompressed_item], "ScannedCount": 1}

    with patch.object(mock_exporter, "_get_table_for_thread", return_value=mock_table):
        mock_exporter.export(str(tmp_path), total_segments=total_segments)

    segment_files = list(tmp_path.glob("segment_*_batch_*.jsonl"))
    assert len(segment_files) == total_segments
    segments_seen = {int(f.name.split("_")[1]) for f in segment_files}
    assert segments_seen == {0, 1, 2}


def test_export_parallel_scan_distributes_limit(mock_exporter, sample_uncompressed_item, tmp_path):
    total_segments = 4
    limit = 10

    captured_scan_kwargs = []
    original_items = [{"PK0": f"Resource:{i}"} for i in range(3)]

    def mock_scan(**kwargs):
        captured_scan_kwargs.append(kwargs)
        return {"Items": original_items, "ScannedCount": 3}

    mock_table = Mock()
    mock_table.scan.side_effect = mock_scan

    with patch.object(mock_exporter, "_get_table_for_thread", return_value=mock_table):
        mock_exporter.export(str(tmp_path), limit=limit, total_segments=total_segments)

    per_segment_limit = math.ceil(limit / total_segments)
    assert per_segment_limit == 3
    for kwargs in captured_scan_kwargs:
        assert kwargs.get("TotalSegments") == total_segments


def test_export_sequential_scan_creates_batch_files(mock_exporter, sample_uncompressed_item, tmp_path):
    mock_exporter.table.scan.return_value = {
        "Items": [sample_uncompressed_item],
        "ScannedCount": 1,
    }

    mock_exporter.export(str(tmp_path), total_segments=1)

    batch_files = list(tmp_path.glob("batch_*.jsonl"))
    assert len(batch_files) == 1
    assert not list(tmp_path.glob("segment_*.jsonl"))
