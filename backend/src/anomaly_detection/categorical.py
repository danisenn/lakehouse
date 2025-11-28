import polars as pl

def detect_categorical_anomalies(df: pl.DataFrame, threshold: float = 0.01) -> pl.DataFrame:
    """
    Identifies rows containing rare values in categorical columns.
    
    A value is considered rare if its frequency is below the threshold.
    Returns a DataFrame containing only the anomalous rows.
    """
    anomalous_indices = set()
    
    # Identify categorical columns (string or low-cardinality integers)
    cat_cols = [
        col for col in df.columns 
        if df[col].dtype == pl.Utf8 or (
            df[col].dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64) and df[col].n_unique() < 100
        )
    ]
    
    total_rows = df.height
    if total_rows == 0:
        return df.head(0)

    for col in cat_cols:
        try:
            # Calculate value counts
            counts = df[col].value_counts()
            
            # Filter for rare values
            # count / total < threshold
            rare_values = counts.filter(
                (pl.col("count") / total_rows) < threshold
            )[col]
            
            if rare_values.len() > 0:
                # Find rows with these rare values
                # We use is_in to find rows where the column value is in the set of rare values
                rare_rows = df.with_row_count().filter(
                    pl.col(col).is_in(rare_values)
                )
                
                # Add indices to our set
                for row in rare_rows.select("row_nr").to_series().to_list():
                    anomalous_indices.add(row)
                    
        except Exception:
            # Skip columns that cause errors (e.g. all nulls)
            continue
            
    if not anomalous_indices:
        return df.head(0)
        
    # Return the rows corresponding to the anomalous indices
    # We need to sort indices to maintain order or just filter
    # Using filter with is_in on row_nr is efficient
    return df.with_row_count().filter(
        pl.col("row_nr").is_in(list(anomalous_indices))
    ).drop("row_nr")
