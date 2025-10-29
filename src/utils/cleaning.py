
# used for cleaninng column names in inference
def clean_column_name(name: str) -> str:
    """Säubert Spaltennamen (Trim, Sonderzeichen entfernen)."""
    return name.strip().lower().replace(' ', '_')