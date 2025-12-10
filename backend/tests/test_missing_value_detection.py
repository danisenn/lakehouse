import polars as pl
import pytest
from src.anomaly_detection.missing_values import detect_missing_value_anomalies


def test_detect_missing_value_anomalies_single_null():
    """Test detecting rows with at least one missing value."""
    df = pl.DataFrame({
        'a': [1, 2, None, 4, 5],
        'b': [10, 20, 30, None, 50],
        'c': [100, 200, 300, 400, 500]
    })
    
    result = detect_missing_value_anomalies(df, threshold=1)
    
    # Should find rows at index 2 and 3 (both have at least 1 null)
    assert result.height == 2
    assert result['a'].to_list() == [None, 4]
    assert result['b'].to_list() == [30, None]


def test_detect_missing_value_anomalies_multiple_nulls():
    """Test detecting rows with multiple missing values."""
    df = pl.DataFrame({
        'a': [1, None, None, 4, 5],
        'b': [10, None, 30, None, 50],
        'c': [100, None, 300, 400, 500]
    })
    
    # Threshold of 2 should only catch row at index 1 (has 3 nulls)
    result = detect_missing_value_anomalies(df, threshold=2)
    
    assert result.height == 1
    assert result['a'].to_list() == [None]
    assert result['b'].to_list() == [None]
    assert result['c'].to_list() == [None]


def test_detect_missing_value_anomalies_no_nulls():
    """Test that no rows are returned when there are no missing values."""
    df = pl.DataFrame({
        'a': [1, 2, 3, 4, 5],
        'b': [10, 20, 30, 40, 50],
        'c': [100, 200, 300, 400, 500]
    })
    
    result = detect_missing_value_anomalies(df, threshold=1)
    
    assert result.height == 0
    assert result.columns == ['a', 'b', 'c']


def test_detect_missing_value_anomalies_empty_dataframe():
    """Test with an empty DataFrame."""
    df = pl.DataFrame({
        'a': [],
        'b': [],
        'c': []
    })
    
    result = detect_missing_value_anomalies(df)
    
    assert result.height == 0


def test_detect_missing_value_anomalies_all_nulls():
    """Test with a DataFrame where all values are null."""
    df = pl.DataFrame({
        'a': [None, None, None],
        'b': [None, None, None],
        'c': [None, None, None]
    })
    
    result = detect_missing_value_anomalies(df, threshold=1)
    
    # All rows have at least 1 null (actually 3 nulls each)
    assert result.height == 3


def test_detect_missing_value_anomalies_threshold_higher_than_columns():
    """Test with threshold higher than number of columns."""
    df = pl.DataFrame({
        'a': [None, 2, 3],
        'b': [None, 20, 30],
    })
    
    # Threshold of 3 is higher than number of columns (2)
    result = detect_missing_value_anomalies(df, threshold=3)
    
    # No row can have 3 nulls when there are only 2 columns
    assert result.height == 0
