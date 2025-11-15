import polars as pl
import numpy as np
from sklearn.ensemble import IsolationForest
from typing import List


def isolation_forest_anomalies(
    df: pl.DataFrame,
    columns: List[str],
    contamination: float = 0.01,
    n_estimators: int = 100,
    random_state: int = 42,
) -> pl.DataFrame:
    """
    Ermittelt Ausreißer mit Isolation Forest.

    - Nutzt die angegebenen numerischen Spalten als Features.
    - Ignoriert Zeilen mit Null/NaN in den Feature-Spalten beim Fitten, markiert sie standardmäßig nicht als Ausreißer.
    - Gibt ein DataFrame mit nur Ausreißer-Zeilen zurück.
    """
    if not columns:
        raise ValueError("'columns' darf nicht leer sein.")
    for c in columns:
        if c not in df.columns:
            raise KeyError(f"Spalte '{c}' existiert nicht im DataFrame.")

    # Zeilenmaske: nur vollständige Zeilen (keine Nulls) für das Modell verwenden
    complete_mask_expr = pl.all_horizontal([pl.col(c).is_not_null() for c in columns])
    complete_mask = df.select(complete_mask_expr.alias("_ok")).to_series().to_numpy()

    # Feature-Matrix
    X_complete = df.filter(pl.Series(complete_mask)).select(columns).to_numpy()
    if X_complete.size == 0:
        # Keine validen Zeilen -> keine Ausreißer
        return df.clear()

    model = IsolationForest(
        contamination=contamination,
        n_estimators=n_estimators,
        random_state=random_state,
    )
    preds_complete = model.fit_predict(X_complete)  # -1 = Anomalie, 1 = normal

    # Vollständige Ergebnis-Maske in volle Länge zurückprojizieren
    is_anomaly_full = np.zeros(len(df), dtype=bool)
    is_anomaly_full[complete_mask] = (preds_complete == -1)

    # Ergebnis-DataFrame mit Flag (optional nützlich)
    df_flagged = df.with_columns(pl.Series("is_anomaly", is_anomaly_full))
    return df_flagged.filter(pl.col("is_anomaly"))
