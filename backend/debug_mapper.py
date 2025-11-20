import polars as pl
from src.semantic_field_mapping import SemanticFieldMapper, map_columns

def debug_duplicate_reference():
    print("Debugging test_duplicate_reference_names_different_case")
    df = pl.DataFrame({"Email": ["a@example.com"]})
    reference_fields = ["EMAIL", "email"]
    
    mapper = SemanticFieldMapper(reference_fields=reference_fields)
    print(f"Mapper created with refs: {mapper.reference_fields}")
    
    cands = mapper._scores_for_column("Email", df["Email"])
    print("Candidates for 'Email':")
    for c in cands:
        print(f"  {c.ref}: {c.score} (details: {c.details})")
        
    res = mapper.map_columns(df)
    print("Result mapping:", res["mapping"])

def debug_list_only():
    print("\nDebugging test_mapping_from_column_list_without_df")
    cols = ["Label", "Title", "ReviewText"]
    refs = ["label", "title", "text"]
    
    mapper = SemanticFieldMapper(reference_fields=refs)
    cands = mapper._scores_for_column("ReviewText", None)
    print("Candidates for 'ReviewText':")
    for c in cands:
        print(f"  {c.ref}: {c.score} (details: {c.details})")

if __name__ == "__main__":
    debug_duplicate_reference()
    debug_list_only()
