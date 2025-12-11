import polars as pl
from typing import Dict, Any, List

def calculate_missing_ratios(df: pl.DataFrame) -> Dict[str, float]:
    """
    Calculates the ratio of missing values per column.
    
    Missing values include:
    - NULL values (np.nan in source data)
    - Empty strings ("") in string columns (common from CSV imports)
    """
    missing_ratios = {}
    for col in df.columns:
        # Count NULL values
        missing_count = df[col].null_count()
        
        # For string columns, also count empty strings
        if df[col].dtype == pl.Utf8:
            empty_string_count = (df[col] == "").sum()
            missing_count += empty_string_count
        
        ratio = missing_count / df.height if df.height > 0 else 0.0
        missing_ratios[col] = ratio
    return missing_ratios

def calculate_numeric_stats(df: pl.DataFrame) -> Dict[str, Dict[str, float]]:
    """Calculates mean, min, max, std for numeric columns."""
    stats = {}
    for col in df.columns:
        if df[col].dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64):
            try:
                # Use drop_nulls to avoid issues with all-null columns
                series = df[col].drop_nulls()
                if series.is_empty():
                    continue
                    
                stats[col] = {
                    "mean": series.mean(),
                    "min": series.min(),
                    "max": series.max(),
                    "std": series.std() if series.len() > 1 else 0.0,
                    "zeros": (series == 0).sum()
                }
            except Exception:
                pass
    return stats

def calculate_text_stats(df: pl.DataFrame, sample_limit: int = 10000) -> Dict[str, Dict[str, Any]]:
    """Calculates unique count and top values for text columns."""
    stats = {}
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            try:
                series = df[col].head(sample_limit) # Limit for performance
                n_unique = series.n_unique()
                
                # Get top 3 values
                top_values = []
                if not series.is_empty():
                    counts = series.value_counts().sort("count", descending=True).head(3)
                    top_values = [
                        {"value": str(row[col]), "count": row["count"]} 
                        for row in counts.to_dicts()
                    ]

                stats[col] = {
                    "unique_count": n_unique,
                    "top_values": top_values
                }
            except Exception:
                pass
    return stats

def detect_categorical(df: pl.DataFrame, threshold: float = 0.05) -> List[str]:
    """Detects categorical columns based on unique value ratio."""
    categorical_cols = []
    for col in df.columns:
        # Check both string and integer columns
        if df[col].dtype in (pl.Utf8, pl.Int8, pl.Int16, pl.Int32, pl.Int64):
            try:
                n_unique = df[col].n_unique()
                total = df.height
                if total > 0 and (n_unique / total) < threshold and n_unique < 100: # Cap at 100 unique values
                    categorical_cols.append(col)
            except Exception:
                pass
    return categorical_cols
