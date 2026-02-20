import polars as pl
import numpy as np
from typing import Optional


def z_score_anomalies(df: pl.DataFrame, column: str, threshold: float = 3.0) -> pl.DataFrame:
    """
    Finds outliers based on the Z-Score.

    - Calculates Z-Scores for the specified numeric column.
    - Returns all rows where the absolute Z-Score > threshold.
    - Handles NaN values robustly (ignored in mean/std calculation; NaN remains non-anomaly).
    """
    if column not in df.columns:
        raise KeyError(f"Column '{column}' does not exist in the DataFrame.")

    values = df[column].to_numpy()
    # Robuste Statistik mit NaN-Handling
    mean = np.nanmean(values)
    std = np.nanstd(values)
    if std == 0 or np.isnan(std):  # No variance or only NaNs
        return df.clear()

    z_scores = (values - mean) / std
    mask = np.abs(z_scores) > threshold
    # Filter expects a Polars Expr or Series with row length
    return df.filter(pl.Series(mask))


def iqr_anomalies(df: pl.DataFrame, column: str) -> pl.DataFrame:
    """
    Finds outliers based on the Interquartile Range (IQR).

    - Values outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR] are marked as outliers.
    - NaN values are ignored.
    """
    if column not in df.columns:
        raise KeyError(f"Column '{column}' does not exist in the DataFrame.")

    values = df[column].to_numpy()
    # Falls alles NaN oder leer
    if values.size == 0 or np.all(np.isnan(values)):
        return df.clear()

    q1 = np.nanpercentile(values, 25)
    q3 = np.nanpercentile(values, 75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    mask = (values < lower) | (values > upper)
    return df.filter(pl.Series(mask))
