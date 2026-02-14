import numpy as np
import pandas as pd

from typing import Tuple, Dict, Any

def add_missing_values(df: pd.DataFrame, fraction: float) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Introduce missing values randomly across all columns.
    fraction = percentage of rows affected (0.0 - 1.0)
    """
    df = df.copy()
    n_rows = len(df)
    n_missing = int(n_rows * fraction)

    # Random rows
    missing_rows = np.random.choice(df.index, n_missing, replace=False)

    for col in df.columns:
        df.loc[missing_rows, col] = np.nan

    metadata = {
        "missing_values": {
            "rows": missing_rows.tolist(),
            "columns": df.columns.tolist()
        }
    }

    return df, metadata