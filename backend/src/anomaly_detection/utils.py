import polars as pl
import numpy as np
from typing import List, Literal, Optional, Sequence

from .rules import z_score_anomalies, iqr_anomalies
from .isolation_forest import isolation_forest_anomalies


Method = Literal["zscore", "iqr", "isolation_forest"]


def select_numeric_columns(df: pl.DataFrame, exclude: Optional[Sequence[str]] = None) -> List[str]:
    """
    Wählt numerische Spalten aus einem Polars-DataFrame aus.

    - exclude: optionale Liste von Spalten, die ausgeschlossen werden sollen.
    """
    exclude = set(exclude or [])
    numeric_dtypes = {pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64}
    cols = [name for name, dtype in df.schema.items() if dtype in numeric_dtypes and name not in exclude]
    return cols


def ensure_columns_exist(df: pl.DataFrame, columns: Sequence[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(f"Spalten fehlen im DataFrame: {missing}")


def detect_anomalies(
    df: pl.DataFrame,
    method: Method,
    columns: Optional[Sequence[str]] = None,
    threshold: float = 3.0,
    contamination: float = 0.01,
    n_estimators: int = 100,
    random_state: int = 42,
) -> pl.DataFrame:
    """
    Führt Anomalieerkennung auf dem gegebenen DataFrame durch und gibt die als Ausreißer
    erkannten Zeilen zurück.

    Parameter je Methode:
    - method="zscore": nutzt eine einzelne Spalte und den Z-Score-Schwellenwert `threshold`.
    - method="iqr": nutzt eine einzelne Spalte (Interquartilsabstand-Regel).
    - method="isolation_forest": nutzt mehrere Spalten und Parameter `contamination`, `n_estimators`, `random_state`.

    Rückgabe: DataFrame mit den Zeilen, die als Anomalien markiert wurden.
    """
    if method in ("zscore", "iqr"):
        if not columns or len(columns) != 1:
            raise ValueError("Für method='zscore' oder 'iqr' muss genau eine Spalte angegeben werden (columns=[...]).")
        col = columns[0]
        if method == "zscore":
            return z_score_anomalies(df, col, threshold=threshold)
        else:
            return iqr_anomalies(df, col)

    if method == "isolation_forest":
        if not columns or len(columns) < 1:
            # Fallback: automatisch numerische Spalten wählen
            columns = select_numeric_columns(df)
        ensure_columns_exist(df, columns)
        return isolation_forest_anomalies(
            df=df,
            columns=list(columns),
            contamination=contamination,
            n_estimators=n_estimators,
            random_state=random_state,
        )

    raise ValueError(f"Unbekannte Methode: {method}. Erlaubt sind 'zscore', 'iqr', 'isolation_forest'.")


__all__ = [
    "select_numeric_columns",
    "ensure_columns_exist",
    "detect_anomalies",
    "z_score_anomalies",
    "iqr_anomalies",
    "isolation_forest_anomalies",
]
