import pytest
import polars as pl
from src.anomaly_detection.utils import detect_anomalies

def test_z_score_anomalies(sample_numeric_df):
    # The 'value' column has 100.0 as an anomaly
    anomalies = detect_anomalies(sample_numeric_df, method="zscore", columns=["value"], threshold=1.5)
    assert anomalies.height == 1
    assert anomalies["value"][0] == 100.0

def test_iqr_anomalies(sample_numeric_df):
    anomalies = detect_anomalies(sample_numeric_df, method="iqr", columns=["value"])
    assert anomalies.height == 1
    assert anomalies["value"][0] == 100.0

def test_isolation_forest_anomalies(sample_numeric_df):
    anomalies = detect_anomalies(
        sample_numeric_df, 
        method="isolation_forest", 
        columns=["id", "value"], 
        contamination=0.2, 
        random_state=42
    )
    assert anomalies.height > 0
    # Isolation forest should flag the outlier
    assert 100.0 in anomalies["value"].to_list()

def test_invalid_method(sample_numeric_df):
    with pytest.raises(ValueError, match="Unknown method"):
        detect_anomalies(sample_numeric_df, method="invalid", columns=["value"])

def test_missing_columns(sample_numeric_df):
    with pytest.raises(KeyError, match="Column 'non_existent' does not exist in the DataFrame."):
        detect_anomalies(sample_numeric_df, method="zscore", columns=["non_existent"], threshold=2.0)
