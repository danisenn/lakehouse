import json
import os
from pathlib import Path

def generate_latex_tables(benchmark_dir: str, output_file: str):
    benchmark_path = Path(benchmark_dir)
    results = []

    for file_path in benchmark_path.glob("*_benchmark_results.json"):
        with open(file_path, "r") as f:
            data = json.load(f)
            table_name = data.get("table_name", file_path.stem.replace("_benchmark_results", ""))
            
            # Use the overall results to build a detailed breakdown per table
            table_latex = f"\\begin{{table}}[ht]\n\\centering\n\\small\n\\begin{{tabular}}{{|l|c|c|c|c|c|c|}}\n\\hline\n"
            table_latex += "\\textbf{{Scenario}} & \\textbf{{Anom P}} & \\textbf{{Anom R}} & \\textbf{{Anom F1}} & \\textbf{{Schema}} & \\textbf{{Sem F1}} & \\textbf{{Map Acc}} \\\\\n\\hline\n"
            
            scenarios = []
            for scenario, metrics in data.items():
                if not isinstance(metrics, dict) or "schema" not in metrics:
                    continue  # Skip any non-scenario keys
                anom = metrics.get('anomaly', {})
                schema = metrics.get('schema', {})
                semantic = metrics.get('semantic', {})
                mapping = metrics.get('mapping', {})
                
                # Format metrics (converting from 0-1 range to percentage strings)
                def pct(val): 
                    return f"{val*100:.2f}\\%" if val is not None else "0.00\\%"
                
                row_str = f"{scenario.replace('_', '\\_')} & {pct(anom.get('precision'))} & {pct(anom.get('recall'))} & {pct(anom.get('f1'))} & {pct(schema.get('accuracy'))} & {pct(semantic.get('f1'))} & {pct(mapping.get('accuracy'))} \\\\"
                table_latex += row_str + "\n"
                
                # Store for aggregate calculation
                scenarios.append({
                    "scenario": scenario,
                    "table": table_name,
                    "metrics": {
                        "anom_f1": anom.get('f1', 0) or 0,
                        "schema_acc": schema.get('accuracy', 0) or 0,
                        "semantic_f1": semantic.get('f1', 0) or 0,
                        "map_acc": mapping.get('accuracy', 0) or 0
                    }
                })
                
            table_latex += "\\hline\n\\end{{tabular}}\n"
            table_latex += f"\\caption{{Benchmark Results for {table_name.replace('_', '\\_')}}}\n"
            table_latex += f"\\label{{tab:bench_{table_name}}}\n\\end{{table}}\n"
            
            results.append({
                "table_name": table_name,
                "latex": table_latex,
                "scenarios": scenarios
            })

    # Prepare final TeX output
    with open(output_file, "w") as out:
        out.write("% Auto-generated Benchmark Report Tables\n\n")
        
        # 1. Write the aggregate summary table
        if results:
            out.write("% --- Aggregate Summary Table ---\n")
            out.write("\\begin{table}[ht]\n\\centering\n\\begin{tabular}{|l|c|c|c|c|}\n\\hline\n")
            out.write("\\textbf{Scenario Level} & \\textbf{Avg Anom F1} & \\textbf{Avg Schema Acc} & \\textbf{Avg Sem F1} & \\textbf{Avg Map Acc} \\\\\n\\hline\n")
            
            # Calculate averages across all tables per scenario
            scenario_totals = {}
            for res in results:
                for s in res["scenarios"]:
                    sn = s["scenario"]
                    if sn not in scenario_totals:
                        scenario_totals[sn] = {"count": 0, "anom_f1": 0, "schema_acc": 0, "semantic_f1": 0, "map_acc": 0}
                    
                    scenario_totals[sn]["count"] += 1
                    scenario_totals[sn]["anom_f1"] += s["metrics"]["anom_f1"]
                    scenario_totals[sn]["schema_acc"] += s["metrics"]["schema_acc"]
                    scenario_totals[sn]["semantic_f1"] += s["metrics"]["semantic_f1"]
                    scenario_totals[sn]["map_acc"] += s["metrics"]["map_acc"]
            
            def avg_pct(total, count):
                return f"{(total/count)*100:.2f}\\%" if count > 0 else "0.00\\%"
                
            for sn, totals in scenario_totals.items():
                c = totals["count"]
                row = f"{sn.replace('_', '\\_')} & {avg_pct(totals['anom_f1'], c)} & {avg_pct(totals['schema_acc'], c)} & {avg_pct(totals['semantic_f1'], c)} & {avg_pct(totals['map_acc'], c)} \\\\"
                out.write(row + "\n")
                
            out.write("\\hline\n\\end{tabular}\n")
            out.write("\\caption{Aggregated Benchmark Averages Across All Tables}\n")
            out.write("\\label{tab:bench_aggregate}\n\\end{table}\n\n\\vspace{2em}\n\n")
        
        # 2. Write individual tables
        out.write("% --- Detailed Table Results ---\n")
        for res in results:
            out.write(res["latex"] + "\n\n")
            
    print(f"✓ Formatted {len(results)} tables and saved to {output_file}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Convert benchmark JSONs to LaTeX sections')
    parser.add_argument('--input', type=str, default='test/data/benchmarks', help='Directory containing JSON results')
    parser.add_argument('--output', type=str, default='tex/tables/benchmark_results.tex', help='Output TeX file path')
    
    args = parser.parse_args()
    
    # Ensure output directory exists
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    
    generate_latex_tables(args.input, args.output)
