import base64
import json
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

    exporter = GenericDynamodbExporter("test-profile", "resources")

    assert exporter.table is None
    assert exporter.table_name is None


def test_detect_compression_with_compressed_data(sample_compressed_item):
    with patch("commands.services.dynamodb_exporter.boto3.Session") as mock_session:
        mock_client = Mock()
        mock_client.list_tables.return_value = {"TableNames": []}
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        exporter = GenericDynamodbExporter(None, ".*")
        exporter.table = Mock()

        is_compressed = exporter._detect_compression(sample_compressed_item)

        assert is_compressed is True


def test_detect_compression_with_binary_type():
    with patch("commands.services.dynamodb_exporter.boto3.Session") as mock_session:
        mock_client = Mock()
        mock_client.list_tables.return_value = {"TableNames": []}
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        exporter = GenericDynamodbExporter(None, ".*")
        exporter.table = Mock()

        data = {"key": "value", "nested": {"field": "data"}}
        data_str = json.dumps(data)
        compress_obj = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        compressed_data = compress_obj.compress(data_str.encode()) + compress_obj.flush()
        binary_data = Binary(compressed_data)

        item_with_binary = {
            "PK0": "Resource:789",
            "SK0": "Resource:789",
            "data": binary_data,
        }

        is_compressed = exporter._detect_compression(item_with_binary)

        assert is_compressed is True


def test_detect_compression_with_uncompressed_data(sample_uncompressed_item):
    with patch("commands.services.dynamodb_exporter.boto3.Session") as mock_session:
        mock_client = Mock()
        mock_client.list_tables.return_value = {"TableNames": []}
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        exporter = GenericDynamodbExporter(None, ".*")
        exporter.table = Mock()

        is_compressed = exporter._detect_compression(sample_uncompressed_item)

        assert is_compressed is False


def test_decompress_data():
    with patch("commands.services.dynamodb_exporter.boto3.Session") as mock_session:
        mock_client = Mock()
        mock_client.list_tables.return_value = {"TableNames": []}
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        exporter = GenericDynamodbExporter(None, ".*")
        exporter.table = Mock()

        original_data = {"key": "value", "nested": {"field": "data"}}
        data_str = json.dumps(original_data)
        compress_obj = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        compressed_data = compress_obj.compress(data_str.encode()) + compress_obj.flush()
        encoded_data = base64.b64encode(compressed_data).decode()

        decompressed = exporter._decompress_data(encoded_data)

        assert decompressed == original_data


def test_process_item_with_compression(sample_compressed_item):
    with patch("commands.services.dynamodb_exporter.boto3.Session") as mock_session:
        mock_client = Mock()
        mock_client.list_tables.return_value = {"TableNames": []}
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        exporter = GenericDynamodbExporter(None, ".*")
        exporter.table = Mock()

        processed = exporter._process_item(sample_compressed_item)

        assert "@data_decompressed" in processed
        assert processed["@data_decompressed"]["key"] == "value"
        assert processed["PK0"] == "Resource:123"
        assert processed["someField"] == "someValue"


def test_process_item_with_binary_compression():
    with patch("commands.services.dynamodb_exporter.boto3.Session") as mock_session:
        mock_client = Mock()
        mock_client.list_tables.return_value = {"TableNames": []}
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        exporter = GenericDynamodbExporter(None, ".*")
        exporter.table = Mock()

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

        processed = exporter._process_item(item_with_binary)

        assert "@data_decompressed" in processed
        assert processed["@data_decompressed"]["key"] == "value"
        assert processed["@data_decompressed"]["nested"]["field"] == "data"
        assert processed["PK0"] == "Resource:789"
        assert processed["someField"] == "someValue"


def test_process_item_without_compression(sample_uncompressed_item):
    with patch("commands.services.dynamodb_exporter.boto3.Session") as mock_session:
        mock_client = Mock()
        mock_client.list_tables.return_value = {"TableNames": []}
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        exporter = GenericDynamodbExporter(None, ".*")
        exporter.table = Mock()

        processed = exporter._process_item(sample_uncompressed_item)

        assert "@data_decompressed" not in processed
        assert processed == sample_uncompressed_item


def test_save_items_to_file(sample_uncompressed_item, tmp_path):
    with patch("commands.services.dynamodb_exporter.boto3.Session") as mock_session:
        mock_client = Mock()
        mock_client.list_tables.return_value = {"TableNames": []}
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        exporter = GenericDynamodbExporter(None, ".*")
        exporter.table = Mock()
        exporter.output_folder = str(tmp_path)

        items = [sample_uncompressed_item]
        exporter._save_items_to_file(items, 1)

        output_file = tmp_path / "batch_00001.jsonl"
        assert output_file.exists()

        with open(output_file) as file:
            lines = file.readlines()
            assert len(lines) == 1
            saved_item = json.loads(lines[0])
            assert saved_item == sample_uncompressed_item


def test_save_mixed_compressed_and_uncompressed_items(
    sample_compressed_item, sample_uncompressed_item, tmp_path
):
    with patch("commands.services.dynamodb_exporter.boto3.Session") as mock_session:
        mock_client = Mock()
        mock_client.list_tables.return_value = {"TableNames": []}
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        exporter = GenericDynamodbExporter(None, ".*")
        exporter.table = Mock()
        exporter.output_folder = str(tmp_path)

        items = [sample_compressed_item, sample_uncompressed_item]
        exporter._save_items_to_file(items, 1)

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
