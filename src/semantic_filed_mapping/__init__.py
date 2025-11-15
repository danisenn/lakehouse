"""
Semantic Field Mapping package.

Core entrypoints:
- SemanticFieldMapper: class performing semantic mapping between dataset columns and reference fields
- map_columns: convenience function wrapping SemanticFieldMapper.map_columns

Usage example:

```
from src.semantic_filed_mapping import SemanticFieldMapper, map_columns
import polars as pl

# Load some CSV
df = pl.read_csv("data/exported_files/amazon_review_polarity.csv", has_header=False).rename({
    "column_1": "label",
    "column_2": "title",
    "column_3": "text",
})

refs = ["label", "title", "text"]
result = map_columns(df=df, reference_fields=refs)
print(result["mapping"])  # {'label': {...}, 'title': {...}, 'text': {...}}
```
"""
from .mapper import SemanticFieldMapper, map_columns

__all__ = ["SemanticFieldMapper", "map_columns"]
