from fuzzywuzzy import process

def find_similar_columns(schema_map, threshold=85):
    """Detect columns that are semantically similar across tables."""
    col_table_map = {}
    for table, columns in schema_map.items():
        for col in columns:
            normalized = col["column_name"].lower().replace("_", "")
            col_table_map.setdefault(normalized, []).append(table)

    # Fuzzy match pairs
    similar_pairs = []
    cols = list(col_table_map.keys())
    for i, col1 in enumerate(cols):
        for col2 in cols[i+1:]:
            score = process.extractOne(col1, [col2])[1]
            if score >= threshold:
                similar_pairs.append((col1, col2, score))
    return similar_pairs

def detect_key_candidates(conn, table_name, column_name):
    """Return True if column is unique (primary key candidate)."""
    query = f"""
    SELECT COUNT(DISTINCT "{column_name}") = COUNT(*) AS is_unique
    FROM "{table_name}"
    """
    result = conn.client.query(query).to_pandas()
    return result.iloc[0]['is_unique']


test = find_similar_columns("lakehouse_semantic_map.json")