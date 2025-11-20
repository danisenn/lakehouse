import re

# used for cleaninng column names in inference
def clean_column_name(name: str) -> str:
    """Säubert Spaltennamen (Trim, Sonderzeichen entfernen)."""
    # Insert underscore before capital letters (CamelCase -> Snake_Case)
    s = re.sub(r'(?<!^)(?=[A-Z])', '_', name.strip())
    return s.lower().replace(' ', '_')