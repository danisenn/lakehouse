import re

with open("backend/src/assistant/runner.py", "r") as f:
    content = f.read()

# 1. Extract the anomaly detection block
start_marker = "    # Anomaly detection"
end_marker = "    return DatasetReport("
start_idx = content.find(start_marker)
end_idx = content.find(end_marker)

anomaly_block = content[start_idx:end_idx]

# Remove the initial indentation from the anomaly block for the helper function body
helper_body = ""
for line in anomaly_block.splitlines():
    if line.startswith("    "):
        helper_body += line[4:] + "\n"
    elif not line.strip():
        helper_body += "\n"
    else:
        helper_body += line + "\n"

# Add anomaly_explanation init inside helper body
helper_body = helper_body.replace(
    'anomalies_counts: Dict[str, int] = {}',
    'anomalies_counts: Dict[str, int] = {}\n    anomaly_explanation: Optional[str] = None'
)

# Fix dataset.name -> dataset_name inside maybe_save and LLM call
helper_body = helper_body.replace('dataset.name', 'dataset_name')

# Fix LLM insight modification
helper_body = helper_body.replace(
    'llm_insights["anomaly_explanation"] = explanation',
    'anomaly_explanation = explanation'
)

# Fix numeric_cols definition: it's passed as arg, don't redefine
helper_body = helper_body.replace(
    'numeric_cols = select_numeric_columns(df_anom, exclude=["row_idx"])\n',
    ''
)

# Complete the helper func body with return
helper_body += "    return anomalies_counts, anomalies_saved, anomalies_rows, anomalies_previews, anomaly_explanation\n"

# Helper funct definition
helper_func = f"""from typing import Tuple, Any

def _detect_all_anomalies(
    dataset_name: str,
    df_anom: pl.DataFrame,
    schema: Dict[str, str],
    mapping_cfg: MappingConfig,
    anomaly_cfg: AnomalyConfig,
    numeric_cols: List[str],
    save_dir: Optional[Path],
    save_samples_limit: int,
) -> Tuple[Dict[str, int], Dict[str, Optional[str]], Dict[str, List[int]], Dict[str, List[Dict[str, Any]]], Optional[str]]:
{helper_body}
"""

run_idx = content.find("def run_on_dataset(")
content = content[:run_idx] + helper_func + content[run_idx:]

new_call = """    # Anomaly detection
    numeric_cols = select_numeric_columns(df_anom, exclude=["row_idx"])
    (
        anomalies_counts,
        anomalies_saved,
        anomalies_rows,
        anomalies_previews,
        anomaly_explanation,
    ) = _detect_all_anomalies(
        dataset_name=dataset.name,
        df_anom=df_anom,
        schema=schema,
        mapping_cfg=mapping_cfg,
        anomaly_cfg=anomaly_cfg,
        numeric_cols=numeric_cols,
        save_dir=save_dir,
        save_samples_limit=save_samples_limit,
    )
    if anomaly_explanation:
        llm_insights["anomaly_explanation"] = anomaly_explanation

"""

start_idx = content.find(start_marker)
end_idx = content.find(end_marker)
content = content[:start_idx] + new_call + content[end_idx:]

with open("backend/src/assistant/runner.py", "w") as f:
    f.write(content)

print("Refactored runner.py successfully!")
