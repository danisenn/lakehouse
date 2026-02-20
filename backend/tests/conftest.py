import pytest
import polars as pl

@pytest.fixture
def sample_numeric_df():
    """Returns a simple Polars DataFrame with numeric columns for testing."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "value": [10.5, 12.0, 11.5, 100.0, 10.9],  # 100.0 is an anomaly
        "category": ["A", "A", "B", "A", "C"],
        "missing_col": [1.0, None, 3.0, None, 5.0]
    })
