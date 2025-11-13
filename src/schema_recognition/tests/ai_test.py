# ai_test_ollama.py
import os
import json
import pandas as pd
import subprocess

def infer_schema_with_ollama(model_name: str, csv_path: str, max_rows: int = 20) -> str:
    """
    Verwendet Ollama lokal, um ein Schema aus CSV zu generieren.
    """
    df = pd.read_csv(csv_path)
    df_sample = df.head(max_rows)
    csv_text = df_sample.to_csv(index=False)

    # prompt = (
    #     f"Hier ist eine CSV-Tabelle:\n{csv_text}\n\n"
    #     "Bitte erstelle ein JSON-Schema mit den Spaltennamen, Datentypen "
    #     "und möglichen Bedeutungen der Spalten. Antworte nur mit gültigem JSON."
    #     "\n/bye"
    # )
    prompt = (
        f"Hier ist eine CSV-Tabelle:\n{csv_text}\n\n"
        "Bitte erstelle ein JSON-Schema und antworte **nur mit gültigem JSON**. "
        "Das Schema muss folgendes Format haben:\n\n"
        "{\n"
        '  "columns": [\n'
        "    {\n"
        '      "name": "Spaltenname",\n'
        '      "type": "Datentyp (z.B. integer, float, string, date)",\n'
        '      "description": "Kurze Beschreibung der Spalte"\n'
        "    },\n"
        "    ...\n"
        "  ]\n"
        "}\n\n"
        "Antworte **nur** mit JSON, keine Erklärungen oder Text davor oder danach.\n"
    )

    try:
        # Ollama via CLI aufrufen
        result = subprocess.run(
            ["ollama", "run", model_name, prompt],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print("Fehler bei Ollama-Inferenz:", e)
        print("stderr:", e.stderr)
        return ""

def pipeline(csv_sample_dir: str, output_dir: str, model_name: str = "llama2"):
    os.makedirs(output_dir, exist_ok=True)

    csv_files = [p for p in os.listdir(csv_sample_dir) if p.endswith(".csv")]
    csv_files = [os.path.join(csv_sample_dir, p) for p in csv_files]
    if not csv_files:
        print("Keine CSV-Dateien in", csv_sample_dir)
        return

    for file_path in csv_files:
        base_name = os.path.splitext(os.path.basename(file_path))[0]

        print(f"\nAnalysiere Schema von {base_name} (heuristisch)...")
        try:
            from src.schema_recognition.inference.schema_inference import infer_schema_from_csv
            from src.schema_recognition.output.report_generator import generate_json_schema_report
            schema = infer_schema_from_csv(file_path)
            generate_json_schema_report(schema, os.path.join(output_dir, f"{base_name}_schema.json"))
        except Exception as e:
            print("Fehler bei heuristischer Schemaerkennung:", e)

        print(f"Analysiere Schema von {base_name} (LLM via Ollama)...")
        schema_llm = infer_schema_with_ollama(model_name, file_path, max_rows=20)
        if schema_llm:
            llm_output_path = os.path.join(output_dir, f"{base_name}_schema_llm.json")
            with open(llm_output_path, "w", encoding="utf-8") as f:
                f.write(schema_llm)
            print("LLM Schema gespeichert:", llm_output_path)
        else:
            print("Keine LLM-Ausgabe erhalten.")

if __name__ == "__main__":
    csv_sample_dir = "data/exported_files"
    output_dir = "src/schema_recognition/output/schema"
    model_name = "llama2"  # Modell lokal bei Ollama installiert
    pipeline(csv_sample_dir, output_dir, model_name)
