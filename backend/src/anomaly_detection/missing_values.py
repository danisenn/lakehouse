import polars as pl


def detect_missing_value_anomalies(
    df: pl.DataFrame,
    threshold: int = 1
) -> pl.DataFrame:
    """
    Identifies rows containing missing (null/NaN) or empty string values.
    
    This function detects both:
    - True NULL values (np.nan in source data)
    - Empty strings ("") which often result from CSV exports/imports
    
    Parameters:
    - df: Input DataFrame
    - threshold: Minimum number of missing values per row to flag as anomaly (default: 1)
    
    Returns:
    - DataFrame containing only rows with at least 'threshold' missing values
    """
    if df.height == 0:
        return df.head(0)
    
    # Count missing values per row across all columns
    # Missing = NULL or empty string (common in CSV imports)
    missing_expressions = []
    for col in df.columns:
        # Check if column is string type
        if df[col].dtype == pl.Utf8:
            # For string columns: count NULLs and empty strings
            missing_expr = (pl.col(col).is_null() | (pl.col(col) == "")).cast(pl.Int32)
        else:
            # For non-string columns: only count NULLs
            missing_expr = pl.col(col).is_null().cast(pl.Int32)
        missing_expressions.append(missing_expr)
    
    # Sum missing values across all columns for each row
    missing_count_expr = sum(missing_expressions)
    
    # Add a temporary column with missing value counts
    df_with_counts = df.with_columns(missing_count_expr.alias("_missing_count"))
    
    # Filter rows where missing count >= threshold
    anomalies = df_with_counts.filter(pl.col("_missing_count") >= threshold)
    
    # Drop the temporary column before returning
    return anomalies.drop("_missing_count")
