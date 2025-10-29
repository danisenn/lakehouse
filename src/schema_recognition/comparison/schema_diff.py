def diff_schemas(schema_old: dict, schema_new: dict) -> dict:
    """
    Vergleicht zwei Schemas (dargestellt als Dict Spaltenname -> Datentyp)
    und gibt die Unterschiede zurück.
    """
    added = {}
    removed = {}
    changed = {}

    old_keys = set(schema_old.keys())
    new_keys = set(schema_new.keys())

    for col in new_keys - old_keys:
        added[col] = schema_new[col]

    for col in old_keys - new_keys:
        removed[col] = schema_old[col]

    for col in old_keys & new_keys:
        if schema_old[col] != schema_new[col]:
            changed[col] = {"old": schema_old[col], "new": schema_new[col]}

    return {"added": added, "removed": removed, "changed": changed}


def print_schema_diff(diff: dict):
    print("Schemaänderungen:")
    if diff["added"]:
        print(f"  Hinzugefügt: {diff['added']}")
    if diff["removed"]:
        print(f"  Entfernt: {diff['removed']}")
    if diff["changed"]:
        print(f"  Geändert: {diff['changed']}")
    if not any(diff.values()):
        print("  Keine Änderungen vorhanden.")
