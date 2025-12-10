import polars as pl


def detect_missing_value_anomalies(
    df: pl.DataFrame,
    threshold: int = 1
) -> pl.DataFrame:
    """
    Identifies rows containing missing (null/NaN) values.
    
    Parameters:
    - df: Input DataFrame
    - threshold: Minimum number of missing values per row to flag as anomaly (default: 1)
    
    Returns:
    - DataFrame containing only rows with at least 'threshold' missing values
    """
    if df.height == 0:
        return df.head(0)
    
    # Count null values per row across all columns
    # Create expression that counts nulls in each column, then sum horizontally
    null_count_expr = sum([pl.col(col).is_null().cast(pl.Int32) for col in df.columns])
    
    # Add a temporary column with null counts
    df_with_counts = df.with_columns(null_count_expr.alias("_null_count"))
    
    # Filter rows where null count >= threshold
    anomalies = df_with_counts.filter(pl.col("_null_count") >= threshold)
    
    # Drop the temporary column before returning
    return anomalies.drop("_null_count")
