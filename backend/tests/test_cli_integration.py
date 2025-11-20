import sys
from unittest.mock import MagicMock, patch
import pytest
from scripts.lakehouse_assistant import main

def test_cli_lakehouse_integration():
    # Mock sys.argv
    test_args = ["--lakehouse", "--query", "SELECT * FROM test", "--refs", "col1"]
    
    # Mock dependencies
    with patch("src.connection.connection.get_connection") as mock_get_conn, \
         patch("src.assistant.datasource.LakehouseSQLDataSource") as mock_ds_cls, \
         patch("scripts.lakehouse_assistant.run_assistant") as mock_run:
        
        # Setup mocks
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        
        mock_ds_instance = MagicMock()
        mock_ds_cls.return_value = mock_ds_instance
        
        mock_report = MagicMock()
        mock_report.datasets = []
        mock_report.to_json.return_value = "{}"
        mock_run.return_value = mock_report
        
        # Run main
        ret = main(test_args)
        
        # Verify
        assert ret == 0
        mock_get_conn.assert_called_once()
        mock_ds_cls.assert_called_once_with(connection_uri=mock_conn, query="SELECT * FROM test")
        mock_run.assert_called_once()

def test_cli_lakehouse_missing_query():
    test_args = ["--lakehouse", "--refs", "col1"]
    
    # Should fail because query is missing
    ret = main(test_args)
    assert ret == 1
