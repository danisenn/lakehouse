import numpy as np
import pandas as pd

from typing import Tuple, Dict, Any

def add_invalid_values(df: pd.DataFrame, fraction: float) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Replace a fraction of values with invalid entries:
    - numeric columns → string 'INVALID'
    - categorical/string columns → random numbers or wrong strings
    """
    df = df.copy()

    n_rows = len(df)
    n_invalid = int(n_rows * fraction)
    rows = np.random.choice(df.index, n_invalid, replace=False)

    for col in df.columns:
        if df[col].dtype in ["float64", "int64"]:
            if not df[col].dtype == object:
                df[col] = df[col].astype(object)
            df.loc[rows, col] = "INVALID"
        else:
            if not df[col].dtype == object:
                df[col] = df[col].astype(object)
            df.loc[rows, col] = "999999"

    metadata = {
        "invalid_values": {
            "rows": rows.tolist(),
            "columns": df.columns.tolist()  # Ideally we'd track per column but simplified here
        }
    }

    return df, metadata
