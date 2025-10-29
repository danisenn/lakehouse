import os
from src.schema_recognition.loader.csv_loader import load_csv_samples
from src.schema_recognition.inference.schema_inference import infer_schema_from_csv
from src.schema_recognition.comparison.schema_diff import diff_schemas, print_schema_diff
from src.schema_recognition.output.report_generator import generate_json_schema_report, generate_diff_report

def pipeline(csv_sample_dir: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    csv_files = load_csv_samples(csv_sample_dir)

    inferred_schemas = {}
    for file_path in csv_files:
        print(f"Analysiere Schema von {os.path.basename(file_path)}")
        schema = infer_schema_from_csv(file_path)
        inferred_schemas[file_path] = schema

    base_path = csv_files[0]

    base_schema = inferred_schemas[base_path]

    for path, schema in inferred_schemas.items():
        print(f"Vergleiche Schema {os.path.basename(base_path)} mit {os.path.basename(path)}")
        diff = diff_schemas(base_schema, schema)
        print_schema_diff(diff)

        base_name = os.path.splitext(os.path.basename(path))[0]
        generate_json_schema_report(schema, os.path.join(output_dir, f"{base_name}_schema.json"))
        generate_diff_report(diff, os.path.join(output_dir, f"{base_name}_diff.txt"))

if __name__ == "__main__":
    csv_sample_dir = "data/exported_files"
    output_dir = "src/schema_recognition/output/schema"
    pipeline(csv_sample_dir, output_dir)
