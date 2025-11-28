import requests
import json
from typing import List, Dict, Optional, Any

class LLMClient:
    """
    Client for interacting with a local Ollama instance.
    """
    def __init__(self, base_url: str = "http://localhost:11434", model: str = "llama3"):
        self.base_url = base_url
        self.model = model

    def _generate(self, prompt: str) -> Optional[str]:
        """
        Generic generation method.
        """
        try:
            response = requests.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False
                },
                timeout=30 # Short timeout to avoid blocking too long
            )
            response.raise_for_status()
            return response.json().get("response", "").strip()
        except Exception as e:
            print(f"LLM Generation Error: {e}")
            return None

    def generate_column_description(self, col_name: str, sample_values: List[Any]) -> Optional[str]:
        """
        Generates a short description for a column based on its name and values.
        """
        prompt = (
            f"You are a data analyst. Write a very short (max 10 words) description for a database column.\n"
            f"Column Name: {col_name}\n"
            f"Sample Values: {sample_values[:5]}\n"
            f"Description:"
        )
        return self._generate(prompt)

    def summarize_table(self, table_name: str, schema: Dict[str, str], sample_rows: List[Dict[str, Any]]) -> Optional[str]:
        """
        Generates a summary of what the table represents.
        """
        # Simplify schema for prompt
        schema_str = ", ".join([f"{k} ({v})" for k, v in list(schema.items())[:10]])
        
        prompt = (
            f"You are a data analyst. Write a concise summary (max 2 sentences) of what this dataset represents. Also mention if the header of the dataset is missing.\n"
            f"Dataset Name: {table_name}\n"
            f"Key Columns: {schema_str}\n"
            f"Sample Data: {sample_rows[:3]}\n"
            f"Summary:"
        )
        return self._generate(prompt)

    def explain_anomalies(self, table_name: str, schema: Dict[str, str], anomalies: List[Dict[str, Any]]) -> Optional[str]:
        """
        Generates an explanation for why the provided rows might be considered anomalous.
        """
        # Simplify schema for prompt
        schema_str = ", ".join([f"{k} ({v})" for k, v in list(schema.items())[:10]])
        
        prompt = (
            f"You are a data analyst. You have detected some anomalies in a dataset. Explain WHY these specific rows might be outliers.\n"
            f"Dataset Name: {table_name}\n"
            f"Key Columns: {schema_str}\n"
            f"Anomalous Rows: {anomalies}\n"
            f"Explanation (max 3 sentences):"
        )
        return self._generate(prompt)
