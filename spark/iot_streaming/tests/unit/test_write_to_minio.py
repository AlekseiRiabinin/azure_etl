from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from spark.iot_streaming.src.spark_etl.stream_processor import write_to_minio

def test_write_to_minio_configuration(batch_test_data):
    """Test if the write stream is properly configured."""
    with patch.object(DataFrame, "writeStream") as mock_write_stream:
        mock_stream = Mock()
        mock_stream.format.return_value = mock_stream
        mock_stream.outputMode.return_value = mock_stream
        mock_stream.option.return_value = mock_stream
        mock_write_stream.return_value = mock_stream

        test_path = "s3a://test-bucket/path"
        write_to_minio(batch_test_data, test_path)

        mock_write_stream.assert_called_once()
        mock_stream.format.assert_called_with("delta")
        mock_stream.start.assert_called_with(test_path)
