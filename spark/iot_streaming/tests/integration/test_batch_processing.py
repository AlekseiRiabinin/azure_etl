

# def test_write_to_minio(mocker):
#     mocker.patch("pyspark.sql.DataFrame.writeStream", new_callable=lambda: Mock())
#     write_to_minio(mock_df, "s3a://test")