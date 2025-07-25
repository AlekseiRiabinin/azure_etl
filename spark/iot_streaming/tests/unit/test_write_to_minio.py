import pytest
from unittest.mock import Mock
from pyspark.sql import DataFrame, SparkSession
from spark.iot_streaming.src.spark_etl.stream_processor import write_to_minio


def test_writing_to_minio_config() -> None:
    """Test configuration without execution."""
    mock_query = Mock()
    mock_writer = Mock()
    
    mock_writer.format.return_value = mock_writer
    mock_writer.outputMode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.start.return_value = mock_query
    
    mock_df = Mock(spec=DataFrame)
    mock_df.writeStream = mock_writer

    test_path = "s3a://test-bucket/path"
    result = write_to_minio(mock_df, test_path)

    mock_writer.format.assert_called_once_with("delta")
    mock_writer.outputMode.assert_called_once_with("append")
    mock_writer.option.assert_called_once_with(
        "checkpointLocation", f"{test_path}/_checkpoints"
    )
    mock_writer.start.assert_called_once_with(test_path)
    assert result == mock_query


def test_writing_to_minio_called_with_correct_path() -> None:
    """Ensure correct S3 path is passed to start()."""
    mock_writer = Mock()

    mock_writer.format.return_value = mock_writer
    mock_writer.outputMode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.start.return_value = "dummy_query"

    mock_df = Mock(spec=DataFrame)
    mock_df.writeStream = mock_writer

    path = "s3a://bucket/data"
    write_to_minio(mock_df, path)

    mock_writer.start.assert_called_once_with(path)


def test_writing_to_minio_multiple_calls() -> None:
    """Ensure method chain is correctly called even if invoked multiple times."""
    mock_query = Mock()
    mock_writer = Mock()

    mock_writer.format.return_value = mock_writer
    mock_writer.outputMode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.start.return_value = mock_query

    mock_df = Mock(spec=DataFrame)
    mock_df.writeStream = mock_writer

    path = "s3a://bucket/path"

    for _ in range(3):
        result = write_to_minio(mock_df, path)
        assert result == mock_query


def test_writing_to_minio_handles_invalid_path_gracefully() -> None:
    """Simulate failure when invalid path is passed to .start()."""
    mock_writer = Mock()

    mock_writer.format.return_value = mock_writer
    mock_writer.outputMode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.start.side_effect = Exception("Invalid path")

    mock_df = Mock(spec=DataFrame)
    mock_df.writeStream = mock_writer

    with pytest.raises(Exception, match="Invalid path"):
        write_to_minio(mock_df, "invalid_path")


def test_write_to_minio(parsed_df: DataFrame, tmp_path, mocker, spark: SparkSession) -> bool:
    """Test MinIO write function."""    
    # Create a test DataFrame with a few rows
    test_df = parsed_df.limit(2)
    
    # Test with empty DataFrame
    empty_df = spark.createDataFrame([], parsed_df.schema)
    write_to_minio(empty_df, 0)
    
    # Test with valid data
    output_path = f"file://{tmp_path}/minio_test/"
    mocker.patch("test_smart_meter_etl.write_to_minio", 
                side_effect=lambda df, batch_id: df.write.parquet(output_path))

    write_to_minio(test_df, 1)
    
    # Verify data was written
    written_df = spark.read.parquet(output_path)
    assert written_df.count() == 2
