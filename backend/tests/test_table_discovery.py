import polars as pl
from unittest.mock import MagicMock, patch
import pytest
from src.assistant.datasource import LakehouseSQLDataSource

def test_schema_mode_table_discovery():
    """Test that schema mode discovers and fetches all tables."""
    
    # Mock connection
    mock_conn = MagicMock()
    
    # Mock table discovery query result
    tables_df = pl.DataFrame({"TABLE_NAME": ["table1", "table2", "table3"]})
    
    # Mock table data
    table1_df = pl.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    table2_df = pl.DataFrame({"col1": [3, 4], "col2": ["c", "d"]})
    table3_df = pl.DataFrame({"col1": [5, 6], "col2": ["e", "f"]})
    
    # Setup mock to return different dataframes based on query
    def mock_to_polars(query):
        if "INFORMATION_SCHEMA" in query:
            return tables_df
        elif "table1" in query:
            return table1_df
        elif "table2" in query:
            return table2_df
        elif "table3" in query:
            return table3_df
        else:
            raise ValueError(f"Unexpected query: {query}")
    
    mock_conn.toPolars = mock_to_polars
    
    # Create datasource in schema mode
    ds = LakehouseSQLDataSource(
        connection_uri=mock_conn,
        schema="lakehouse.datalake.raw",
        max_rows=100
    )
    
    # Collect datasets
    datasets = list(ds.iter_datasets())
    
    # Verify
    assert len(datasets) == 3
    assert datasets[0].name == "lakehouse.datalake.raw.table1"
    assert datasets[1].name == "lakehouse.datalake.raw.table2"
    assert datasets[2].name == "lakehouse.datalake.raw.table3"
    assert datasets[0].df.height == 2
    assert datasets[1].df.height == 2
    assert datasets[2].df.height == 2

def test_schema_mode_discovery_error():
    """Test error handling when schema discovery fails."""
    
    mock_conn = MagicMock()
    mock_conn.toPolars.side_effect = Exception("Discovery failed")
    
    ds = LakehouseSQLDataSource(
        connection_uri=mock_conn,
        schema="bad.schema"
    )
    
    datasets = list(ds.iter_datasets())
    
    assert len(datasets) == 1
    assert "discovery_error" in datasets[0].name
    assert "_error" in datasets[0].df.columns

def test_cli_schema_mode():
    """Test CLI with --schema flag."""
    from scripts.lakehouse_assistant import main
    
    test_args = ["--lakehouse", "--schema", "lakehouse.datalake.raw", "--refs", "col1"]
    
    with patch("src.connection.connection.get_connection") as mock_get_conn, \
         patch("src.assistant.datasource.LakehouseSQLDataSource") as mock_ds_cls, \
         patch("scripts.lakehouse_assistant.run_assistant") as mock_run:
        
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        
        mock_ds_instance = MagicMock()
        mock_ds_cls.return_value = mock_ds_instance
        
        mock_report = MagicMock()
        mock_report.datasets = []
        mock_report.to_json.return_value = "{}"
        mock_run.return_value = mock_report
        
        ret = main(test_args)
        
        assert ret == 0
        mock_ds_cls.assert_called_once_with(
            connection_uri=mock_conn,
            schema="lakehouse.datalake.raw",
            max_rows=0
        )
