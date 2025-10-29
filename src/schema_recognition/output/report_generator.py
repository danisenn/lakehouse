import json
from pathlib import Path

def generate_json_schema_report(schema: dict, output_path: str):
    """
    Speichert das erkannte Schema als JSON-Datei.
    """
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=4)
    print(f"Schema-Report gespeichert: {output_path}")


def generate_diff_report(diff: dict, output_path: str):
    """
    Speichert das Schema-Diff als Klartext-Report.
    """
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        if diff["added"]:
            f.write("HINZUGEFÜGTE FELDER:\n")
            for col, dtype in diff["added"].items():
                f.write(f"  {col}: {dtype}\n")
        if diff["removed"]:
            f.write("\nENTFERNTE FELDER:\n")
            for col, dtype in diff["removed"].items():
                f.write(f"  {col}: {dtype}\n")
        if diff["changed"]:
            f.write("\nGEÄNDERTE FELDER:\n")
            for col, change in diff["changed"].items():
                f.write(f"  {col}: {change['old']} -> {change['new']}\n")
        if not any(diff.values()):
            f.write("Keine Änderungen festgestellt.\n")
    print(f"Diff-Report gespeichert: {output_path}")
