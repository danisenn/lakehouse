import pandas as pd
import numpy as np

from typing import Tuple, Dict, Any

def add_format_errors(df: pd.DataFrame, fraction: float) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Introduce formatting issues:
    - Add random whitespace
    - Replace decimal separators
    - Insert special characters
    """
    df = df.copy()

    n_rows = len(df)
    n_err = int(n_rows * fraction)
    rows = np.random.choice(df.index, n_err, replace=False)

    for col in df.columns:
        # Convert to string to cause type-format issues
        df[col] = df[col].astype("object")

        for r in rows:
            value = str(df.loc[r, col])

            options = [
                f" {value}",                   # leading space
                f"{value} ",                   # trailing space
                value.replace(".", ","),       # wrong decimal separator n
                f"*{value}*",                  # add special chars
                value.upper() if value else value
            ]

            df.loc[r, col] = np.random.choice(options)

            df.loc[r, col] = np.random.choice(options)

    metadata = {
        "format_errors": {
            "rows": rows.tolist(),
            "columns": df.columns.tolist()
        }
    }

    return df, metadata
