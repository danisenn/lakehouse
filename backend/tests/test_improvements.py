import pytest
from unittest.mock import MagicMock, patch
import polars as pl
from src.assistant.datasource import LakehouseSQLDataSource, LocalFilesDataSource
from src.schema_recognition.inference.schema_inference import infer_schema_from_csv

def test_lakehouse_datasource():
    with patch("src.assistant.datasource.LakehouseSQLDataSource._execute_query") as mock_exec:
        mock_df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        mock_exec.return_value = mock_df
        
        ds = LakehouseSQLDataSource(connection_uri="sqlite:///:memory:", query="SELECT * FROM table")
        datasets = list(ds.iter_datasets())
        
        assert len(datasets) == 1
        assert datasets[0].name == "lakehouse_query"
        assert datasets[0].df.height == 2
        mock_exec.assert_called_once()

def test_lakehouse_datasource_error():
    with patch("src.assistant.datasource.LakehouseSQLDataSource._execute_query") as mock_exec:
        mock_exec.side_effect = Exception("Connection failed")
        
        ds = LakehouseSQLDataSource(connection_uri="bad_uri", query="SELECT * FROM table")
        datasets = list(ds.iter_datasets())
        
        assert len(datasets) == 1
        assert "error" in datasets[0].name
        assert "_error" in datasets[0].df.columns
        assert datasets[0].df["_error"][0] == "Connection failed"

def test_schema_inference_sampling(tmp_path):
    # Create a dummy CSV
    csv_path = tmp_path / "test.csv"
    with open(csv_path, "w") as f:
        f.write("col1,col2\n")
        for i in range(2000):
            f.write(f"{i},val{i}\n")
            
    # Test with default sampling (should be 1000)
    # We can't easily verify the exact number of rows read by pl.read_csv without mocking it,
    # but we can verify the function runs and returns a schema.
    
    with patch("polars.read_csv") as mock_read_csv:
        mock_df = pl.DataFrame({"col1": [1], "col2": ["val1"]})
        mock_read_csv.return_value = mock_df
        
        schema = infer_schema_from_csv(str(csv_path), sample_rows=500)
        
        mock_read_csv.assert_called_with(str(csv_path), n_rows=500)
        assert "col1" in schema
        assert "col2" in schema

def test_local_datasource_robustness(tmp_path):
    # Create a malformed CSV that might cause issues
    csv_path = tmp_path / "bad.csv"
    with open(csv_path, "w") as f:
        f.write("col1,col2\n1,2\n3") # Malformed
        
    ds = LocalFilesDataSource(root=tmp_path)
    # This shouldn't crash, but might yield an error dataset or a best-effort read
    datasets = list(ds.iter_datasets())
    assert len(datasets) > 0
