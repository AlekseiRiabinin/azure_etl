import pytest
import os
from tempfile import TemporaryDirectory
from pyspark.sql import DataFrame
from your_module import write_to_minio

@pytest.mark.integration
def test_write_to_minio_integration(spark, batch_test_data):
    """Integration test for MinIO write functionality."""
    with TemporaryDirectory() as tmpdir:
        # Create a streaming DF from static data
        stream_df = spark.createDataFrame(
            batch_test_data.rdd,
            schema=batch_test_data.schema
        ).writeStream.format("memory").queryName("test_stream").start()
        
        # Write to temporary directory (mocking MinIO)
        minio_path = f"file://{tmpdir}/output"
        write_query = write_to_minio(stream_df, minio_path)
        write_query.awaitTermination(10)  # Wait briefly for processing
        
        # Verify output
        written_df = spark.read.format("delta").load(minio_path)
        assert written_df.count() == batch_test_data.count()
        assert set(row["meter_id"] for row in written_df.collect()) == {
            "DUNEDIN_222", "HAMILTON_337", "AUCKLAND_218"
        }
        
        # Verify checkpoint was created
        assert os.path.exists(f"{tmpdir}/output/_checkpoints")