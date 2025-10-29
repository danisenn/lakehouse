def detect_nested_structures(df: 'pl.DataFrame'):
    """Gibt die Spalten zurück, die verschachtelte Daten enthalten (Liste, Dikt etc)."""
    nested_cols = []
    for name, dtype in df.schema.items():
        if dtype in ['list', 'struct']:  # polars Datentypen für nested
            nested_cols.append(name)
    return nested_cols
