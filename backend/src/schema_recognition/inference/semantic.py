import re
from typing import Dict, List, Any, Optional
import polars as pl
from src.semantic_field_mapping.patterns import (
    EMAIL_RE, URL_RE, UUID_RE, DATE_RE, DATETIME_RE, PRICE_RE, PHONE_RE
)

# Additional patterns
IP_RE = re.compile(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")
CREDIT_CARD_RE = re.compile(r"^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})$")

class SemanticTypeDetector:
    """
    Detects semantic types of columns based on value patterns.
    """
    
    PATTERNS = {
        "Email": EMAIL_RE,
        "URL": URL_RE,
        "UUID": UUID_RE,
        "Date": DATE_RE,
        "DateTime": DATETIME_RE,
        "Price": PRICE_RE,
        "Phone": PHONE_RE,
        "IP Address": IP_RE,
        "Credit Card": CREDIT_CARD_RE,
    }

    def detect(self, df: pl.DataFrame, sample_size: int = 200) -> Dict[str, str]:
        """
        Returns a dictionary mapping column names to their detected semantic type.
        Only returns columns where a type was confidently detected.
        """
        semantic_types = {}
        
        for col in df.columns:
            # Skip non-string columns for regex matching (mostly)
            # We could cast, but let's stick to string-likes for now
            if not df[col].dtype == pl.Utf8:
                continue
                
            # Sample non-null values
            sample = df[col].drop_nulls().head(sample_size).to_list()
            if not sample:
                continue
            
            best_type = self._match_type(sample)
            if best_type:
                semantic_types[col] = best_type
                
        return semantic_types

    def _match_type(self, values: List[str]) -> Optional[str]:
        """
        Check which pattern matches the majority of values.
        """
        if not values:
            return None
            
        counts = {k: 0 for k in self.PATTERNS.keys()}
        total = len(values)
        
        for val in values:
            val_str = str(val).strip()
            if not val_str:
                continue
            for type_name, pattern in self.PATTERNS.items():
                if pattern.match(val_str):
                    counts[type_name] += 1
        
        # Threshold: > 50% match
        for type_name, count in counts.items():
            if count / total > 0.5:
                return type_name
                
        return None
