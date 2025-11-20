#!/usr/bin/env bash
set -euo pipefail

# Simple smoke test for the lakehouse assistant system.
# Usage: ./scripts/smoke_test_assistant.sh

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

# Ensure deps (best to pre-install via requirements.txt)
python - <<'PY'
import sys
missing = []
for mod in ["polars", "sklearn", "yaml"]:
    try:
        __import__(mod)
    except Exception:
        missing.append(mod)
if missing:
    print("[warn] Missing modules:", ", ".join(missing))
    print("Install via: pip install -r requirements.txt")
PY

# Run with example config targeting data/
python scripts/lakehouse_assistant.py \
  --config configs/assistant_example.yml \
  --refs label,title,text \
  --verbose

# Show report path
echo "\nReport written to: artifacts/assistant_report.json"
