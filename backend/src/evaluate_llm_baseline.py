import os
import json
import logging
import asyncio
from typing import List, Dict, Any, Optional

from src.assistant.llm_client import LLMClient
from src.semantic_field_mapping.mapper import SemanticFieldMapper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# A simulated subset of 100 random column names from varied datasets (Kaggle/Enterprise)
# For the sake of this script, we include a smaller representative sample to demonstrate the pipeline
TEST_COLUMNS = [
    # E-Commerce & Customer Data
    {"source": "cust_id", "ground_truth": "customer_id", "context": "table of buyers"},
    {"source": "first_nme", "ground_truth": "first_name", "context": "user personal info"},
    {"source": "zipcode", "ground_truth": "postal_code", "context": "address info"},
    {"source": "purch_amt", "ground_truth": "purchase_amount", "context": "transaction details"},
    {"source": "email_addr", "ground_truth": "email", "context": "contact data"},
    # Logistics
    {"source": "dlvry_dt", "ground_truth": "delivery_date", "context": "shipping data"},
    {"source": "lng", "ground_truth": "longitude", "context": "gps coordinates"},
    {"source": "lat", "ground_truth": "latitude", "context": "gps coordinates"},
    # Sports Analytics (The difficult ones)
    {"source": "fgm", "ground_truth": "field_goals_made", "context": "basketball player stats"},
    {"source": "fga", "ground_truth": "field_goals_attempted", "context": "basketball player stats"},
    {"source": "fg3m", "ground_truth": "three_point_made", "context": "basketball player stats"},
    {"source": "reb", "ground_truth": "rebounds", "context": "basketball player stats"},
    {"source": "ast", "ground_truth": "assists", "context": "basketball player stats"},
    # Energy
    {"source": "gen_gwh", "ground_truth": "generation_gwh", "context": "power plant output"},
    {"source": "plant_name", "ground_truth": "facility_name", "context": "energy infrastructure"},
]

REFERENCE_FIELDS = [
    "customer_id", "first_name", "postal_code", "purchase_amount", "email",
    "delivery_date", "longitude", "latitude", 
    "field_goals_made", "field_goals_attempted", "three_point_made", "rebounds", "assists",
    "generation_gwh", "facility_name",
    "last_name", "city", "state", "discount_price", "actual_price", "turnovers"
]

class LLMEvaluationPipeline:
    def __init__(self):
        self.mapper = SemanticFieldMapper(
            reference_fields=REFERENCE_FIELDS,
            threshold=0.6,
            epsilon=0.08  # Wider window to simulate ambiguity in hard domains
        )
        self.llm = LLMClient()

    def resolve_ambiguity_with_llm(self, source_col: str, candidates: List[Dict[str, Any]], context: str) -> Optional[str]:
        """
        Custom prompt to resolve ambiguous mappings using the LLM. 
        This simulates the 'tie-breaker' logic described in the thesis.
        """
        cand_list_str = "\n".join([f"- {c['target']} (Score: {c['score']:.2f})" for c in candidates])
        
        prompt = f"""
SYSTEM: You are an expert data architect mapping a raw data lake column to a strict relational schema.
CONTEXT: We are processing a dataset related to: {context}.
TASK: We have a raw column named "{source_col}".
The deterministic matcher is uncertain and has provided these top candidates:
{cand_list_str}

Based on the context and the abbreviation "{source_col}", return exactly and ONLY the name of the correct target candidate from the list above. Do not include any other text or explanations.
"""
        response = self.llm._generate(prompt)
        if not response:
            return None
            
        # Clean response
        clean_resp = response.strip().strip("'\"").split("\n")[0]
        for c in candidates:
            if c["target"].lower() in clean_resp.lower():
                return c["target"]
        return None

    def evaluate(self):
        logger.info(f"Starting Baseline Evaluation over {len(TEST_COLUMNS)} columns...")
        
        results = {
            "classic_only": {"correct": 0, "incorrect": 0, "ambiguous": 0},
            "llm_augmented": {"correct": 0, "incorrect": 0, "failed_llm": 0}
        }
        
        # We process manually to intercept ambiguity
        for test in TEST_COLUMNS:
            source = test["source"]
            true_target = test["ground_truth"]
            context = test["context"]
            
            # 1. Deterministic Mapping
            mapping_result = self.mapper.map_columns([source])
            
            # Check Results
            mapped = mapping_result.get("mapping", {}).get(source)
            ambiguous_cands = mapping_result.get("ambiguous", {}).get(source)
            
            if mapped:
                # Direct Match via Levenshtein/Jaccard
                predicted = mapped["target"]
                if predicted == true_target:
                    results["classic_only"]["correct"] += 1
                    results["llm_augmented"]["correct"] += 1
                else:
                    results["classic_only"]["incorrect"] += 1
                    results["llm_augmented"]["incorrect"] += 1
            
            elif ambiguous_cands:
                # Ambiguous state
                results["classic_only"]["ambiguous"] += 1
                
                # 2. LLM Resolution
                llm_prediction = self.resolve_ambiguity_with_llm(source, ambiguous_cands, context)
                if llm_prediction == true_target:
                    results["llm_augmented"]["correct"] += 1
                else:
                    results["llm_augmented"]["incorrect"] += 1
                    results["llm_augmented"]["failed_llm"] += 1
            else:
                # Unmapped
                results["classic_only"]["incorrect"] += 1
                results["llm_augmented"]["incorrect"] += 1
                
        # Calculate Metrics
        total = len(TEST_COLUMNS)
        acc_classic = results["classic_only"]["correct"] / total
        acc_llm = results["llm_augmented"]["correct"] / total
        
        logger.info("\n--- EVALUATION RESULTS ---")
        logger.info(f"Total Columns Tested: {total}")
        logger.info(f"Classic Deterministic Accuracy: {acc_classic:.2%} ({results['classic_only']['correct']}/{total})")
        logger.info(f"Classic Ambiguous:              {results['classic_only']['ambiguous']} unresolved")
        logger.info(f"LLM Augmented Accuracy:         {acc_llm:.2%} ({results['llm_augmented']['correct']}/{total})")
        logger.info(f"LLM Failed Res:                 {results['llm_augmented']['failed_llm']}")
        
        # Generate LaTeX Table
        latex_table = f"""
\\begin{{table}}[ht]
\\centering
\\caption{{Semantic Mapping Accuracy: Classical vs. LLM-Augmented Baseline (n={total})}}
\\label{{tab:llm_baseline_eval}}
\\begin{{tabular}}{{@{{}}lccc@{{}}}}
\\toprule
\\textbf{{Method}} & \\textbf{{Accuracy}} & \\textbf{{Ambiguous/Unresolved}} & \\textbf{{Incorrect}} \\\\ \\midrule
Deterministic Heuristics & {acc_classic:.2\\%} & {results['classic_only']['ambiguous']} & {results['classic_only']['incorrect']} \\\\
LLM-Augmented (Llama 3) & {acc_llm:.2\\%} & 0 (forced res.) & {results['llm_augmented']['incorrect']} \\\\ \\bottomrule
\\end{{tabular}}
\\end{{table}}
"""
        logger.info("\nGenerated LaTeX Table (paste this into results.tex):\n" + latex_table)

if __name__ == "__main__":
    pipeline = LLMEvaluationPipeline()
    pipeline.evaluate()
