from src.schema_recognition.comparison.schema_diff import diff_schemas, print_schema_diff
from src.schema_recognition.output.report_generator import generate_json_schema_report, generate_diff_report

def test_comparison_and_output():
    schema_v1 = {
        "id": "Int64",
        "name": "String",
        "age": "Int64",
    }

    schema_v2 = {
        "id": "Int64",
        "name": "Utf8",
        "age": "Int64",
        "email": "String"
    }

    # Vergleichen
    diff = diff_schemas(schema_v1, schema_v2)
    print_schema_diff(diff)

    # Reports als Dateien speichern
    generate_json_schema_report(schema_v2, "test_outputs/schema_v2.json")
    generate_diff_report(diff, "test_outputs/schema_diff.txt")

if __name__ == "__main__":
    test_comparison_and_output()
