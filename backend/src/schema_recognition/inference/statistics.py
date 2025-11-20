def calculate_missing_ratios(df: 'pl.DataFrame'):
    """Berechnet den Anteil fehlender Werte pro Spalte."""
    missing_ratios = {}
    for col in df.columns:
        missing_count = df[col].null_count()
        ratio = missing_count / df.height
        missing_ratios[col] = ratio
    return missing_ratios
