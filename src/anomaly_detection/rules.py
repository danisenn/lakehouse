import polars as pl
import numpy as np
from typing import Optional


def z_score_anomalies(df: pl.DataFrame, column: str, threshold: float = 3.0) -> pl.DataFrame:
    """
    Findet Ausreißer basierend auf dem Z-Score.

    - Berechnet Z-Scores für die angegebene numerische Spalte.
    - Gibt alle Zeilen zurück, deren absoluter Z-Score > threshold ist.
    - Handhabt NaN-Werte robust (werden bei Mittelwert/Std ignoriert; NaN bleibt Nicht‑Anomalie).
    """
    if column not in df.columns:
        raise KeyError(f"Spalte '{column}' existiert nicht im DataFrame.")

    values = df[column].to_numpy()
    # Robuste Statistik mit NaN-Handling
    mean = np.nanmean(values)
    std = np.nanstd(values)
    if std == 0 or np.isnan(std):  # Keine Streuung oder nur NaNs
        return df.clear()

    z_scores = (values - mean) / std
    mask = np.abs(z_scores) > threshold
    # Filter erwartet ein Polars-Expr oder Series mit Zeilenlänge
    return df.filter(pl.Series(mask))


def iqr_anomalies(df: pl.DataFrame, column: str) -> pl.DataFrame:
    """
    Findet Ausreißer auf Basis des Interquartilsabstands (IQR).

    - Werte außerhalb [Q1 - 1.5*IQR, Q3 + 1.5*IQR] werden als Ausreißer markiert.
    - NaN-Werte werden ignoriert.
    """
    if column not in df.columns:
        raise KeyError(f"Spalte '{column}' existiert nicht im DataFrame.")

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
