from schema import get_table_schema, list_tables
from collections import defaultdict
import json


def build_schema_map(space_path="lakehouse.datalake.raw"):
    schema_map = {}
    for table_name in list_tables(space_path):
        df = get_table_schema(space_path, table_name)  # <- fixed
        if df is not None:
            schema_map[table_name] = [
                {"column_name": row["COLUMN_NAME"], "data_type": row["DATA_TYPE"]}
                for row in df.iter_rows(named=True)
            ]
    return schema_map


def build_semantic_links(schema_map):
    column_to_tables = defaultdict(list)

    for table, columns in schema_map.items():
        for col in columns:
            col_name = col["column_name"].lower()
            column_to_tables[col_name].append(table)

    # Keep only columns appearing in more than one table
    semantic_links = {col: tables for col, tables in column_to_tables.items() if len(tables) > 1}
    return semantic_links


def generate_semantic_map(space_path="lakehouse.datalake.raw"):
    schema_map = build_schema_map(space_path)
    semantic_links = build_semantic_links(schema_map)

    semantic_map = {
        "tables": schema_map,
        "semantic_links": semantic_links
    }

    # Save to JSON
    with open("lakehouse_semantic_map.json", "w") as f:
        json.dump(semantic_map, f, indent=4)

    print("Semantic map generated and saved to lakehouse_semantic_map.json")
    return semantic_map


semantic_map = generate_semantic_map()
