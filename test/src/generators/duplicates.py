import pandas as pd

from typing import Tuple, Dict, Any

def add_duplicates(df: pd.DataFrame, fraction: float) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Create duplicated rows by sampling existing rows and appending them.
    """
    df = df.copy()

    n_rows = len(df)
    n_dupl = int(n_rows * fraction)

    duplicates = df.sample(n=n_dupl, replace=True)

    df_out = pd.concat([df, duplicates], ignore_index=True)

    # The new rows are appended at the end
    new_indices = list(range(n_rows, len(df_out)))

    metadata = {
        "duplicates": {
            "rows": new_indices,
            "original_rows": duplicates.index.tolist()
        }
    }

    return df_out, metadata
