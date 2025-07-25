def test_streaming_management(mocker):
    """Test streaming query management."""
    mock_pg_query = mocker.MagicMock()
    mock_minio_query = mocker.MagicMock()
    
    mock_pg_query.isActive = True
    mock_minio_query.isActive = True
    
    mocker.patch("test_smart_meter_etl.enhanced_df.writeStream.foreachBatch", 
                side_effect=[mock_pg_query, mock_minio_query])
    
    # Mock sleep to break the infinite loop
    mocker.patch("time.sleep", side_effect=KeyboardInterrupt())
    
    run_streaming()
    
    mock_pg_query.stop.assert_called_once()
    mock_minio_query.stop.assert_called_once()


def run_streaming():
    """Mock implementation for testing."""
    pass
