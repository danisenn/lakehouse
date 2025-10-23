import csv
import json
from pathlib import Path

# --- CONFIGURE FILE PATHS HERE ---
csv_path = Path('/Volumes/Intenso/Master Thesis/data/amazon/Amazon reviews/amazon_review_polarity_csv/train.csv')    # path to your CSV file
json_path = Path('/Volumes/Intenso/Master Thesis/data/amazon/Amazon reviews/amazon_review_polarity.json') # path to save the JSON file
# ---------------------------------

def csv_to_json(csv_file, json_file):
    # Read CSV file
    with open(csv_file, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Write JSON file
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(rows, f, indent=4, ensure_ascii=False)

    print(f"✅ Converted '{csv_file}' → '{json_file}' ({len(rows)} records)")

# Ensure CSV exists
if not csv_path.exists():
    raise FileNotFoundError(f"CSV file not found: {csv_path}")

# Convert CSV to JSON
csv_to_json(csv_path, json_path)
