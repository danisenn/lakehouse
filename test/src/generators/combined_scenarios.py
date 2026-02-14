import pandas as pd

from .missing_values import add_missing_values
from .outliers import add_outliers
from .duplicates import add_duplicates
from .invalid_values import add_invalid_values
from .format_errors import add_format_errors


from typing import Tuple, Dict, Any

def apply_combined_scenario(
        df: pd.DataFrame,
        config: dict
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Apply multiple data quality issues as defined in a config dict:
    Example config:
    {
        "missing_values": 0.1,
        "outliers": 0.05,
        "invalid_values": 0.03,
        "duplicates": 0.1,
        "format_errors": 0.02
    }

    """
    df = df.copy()
    
    combined_metadata = {}

    if "missing_values" in config:
        df, meta = add_missing_values(df, config["missing_values"])
        combined_metadata.update(meta)

    if "outliers" in config:
        df, meta = add_outliers(df, config["outliers"])
        combined_metadata.update(meta)

    if "invalid_values" in config:
        df, meta = add_invalid_values(df, config["invalid_values"])
        combined_metadata.update(meta)

    if "format_errors" in config:
        df, meta = add_format_errors(df, config["format_errors"])
        combined_metadata.update(meta)

    if "duplicates" in config:
        df, meta = add_duplicates(df, config["duplicates"])
        combined_metadata.update(meta)

    return df, combined_metadata
