import numpy as np
import pandas as pd

from typing import Tuple, Dict, Any

def add_outliers(df: pd.DataFrame, fraction: float, multiplier: int = 5) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Introduce numeric outliers by multiplying values with strong factors.
    Only numeric columns are affected.
    """
    df = df.copy()

    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    n_rows = len(df)
    n_outliers = int(n_rows * fraction)

    if len(numeric_cols) == 0:
        return df, {}

    outlier_rows = np.random.choice(df.index, n_outliers, replace=False)

    for col in numeric_cols:
        df.loc[outlier_rows, col] *= multiplier

    metadata = {
        "outliers": {
            "rows": outlier_rows.tolist(),
            "columns": numeric_cols.tolist()
        }
    }

    return df, metadata
