import polars as pl
from typing import List

def refine_col_type(df: pl.DataFrame, col_name: str) -> pl.Expr:
    """
    Attempts to infer a better type for a given string column.
    Returns an expression that casts the column, or the original column expression if no better type is found.
    """
    
    # helper to check if cast is valid
    # valid means: number of nulls after cast is roughly same as before (allowing for empty strings becoming null)
    # But wait, original empty strings -> null in cast?
    # We should filter out empty strings first to verify "real" values.
    
    expr = pl.col(col_name)
    
    # Get non-null, non-empty values
    valid_values = df.filter(
        pl.col(col_name).is_not_null() & (pl.col(col_name) != "")
    )
    
    if valid_values.height == 0:
        return expr # Keep as is (or cast to something? No, Null/String is fine)
    
    # Try Boolean
    # Boolean cast is tricky, Polars supports "true"/"false".
    # Let's check unique values.
    n_unique = valid_values[col_name].n_unique()
    if n_unique <= 2:
        # Check if values are boolean-like
        unique_vals = set(valid_values[col_name].unique().to_list())
        lower_vals = {str(v).lower() for v in unique_vals}
        if lower_vals.issubset({"true", "false", "0", "1", "yes", "no"}):
             # Polars newer versions do not support casting Utf8View to Boolean directly.
             # We must map the strings to boolean values instead.
             try:
                 bool_map = {
                     "true": True, "false": False,
                     "1": True, "0": False,
                     "yes": True, "no": False,
                     "t": True, "f": False,
                     "y": True, "n": False
                 }
                 # Apply lower() and replace with boolean values
                 # We must use strict=False to ensure unmapped values become null 
                 # instead of throwing an error for dirty data
                 casted = expr.str.to_lowercase().replace(bool_map, default=None)
                 
                 # Verify that the casted column hasn't introduced more nulls than before
                 # valid_values was our non-null subset
                 test_casted = valid_values.select(pl.col(col_name).str.to_lowercase().replace(bool_map, default=None))
                 if test_casted.null_count().item() == 0:
                     return casted
             except Exception as e:
                 pass

    # Try Integer
    try:
        # Check if we can cast to Int64 without creating NEW nulls (ignoring original nulls/empties)
        # We use the 'valid_values' subset to test
        
        # Note: '1.0' cannot be cast to Int strictly in some contexts, but Polars strict=False might make it null
        # We want to support "123". "123.0" -> Float usually.
        
        casted = valid_values.select(pl.col(col_name).cast(pl.Int64, strict=False))
        if casted.null_count().item() == 0:
            return expr.cast(pl.Int64, strict=False)
    except:
        pass

    # Try Float
    try:
        casted = valid_values.select(pl.col(col_name).cast(pl.Float64, strict=False))
        if casted.null_count().item() == 0:
            return expr.cast(pl.Float64, strict=False)
    except:
        pass

    # Try Date
    try:
        # try_parse_dates is standard in read_csv, but here we do it per column
        # str.to_date works for ISO and common formats
        casted = valid_values.select(pl.col(col_name).str.to_date(strict=False))
        if casted.null_count().item() == 0:
            return expr.str.to_date(strict=False)
    except:
        pass

    # Try Datetime
    try:
        # We need to handle various formats. str.to_datetime without format string infers?
        # Polars has `str.to_datetime` that takes `time_unit` etc.
        # Ideally we use `str.strptime` if we know format, but we don't.
        # Polars has an inference engine for CSV, but not exposed easily for single col?
        # Let's try generic to_datetime with strict=False
        casted = valid_values.select(pl.col(col_name).str.to_datetime(strict=False))
        if casted.null_count().item() == 0:
            return expr.str.to_datetime(strict=False)
    except:
        pass
        
    return expr

def refine_types(df: pl.DataFrame) -> pl.DataFrame:
    """
    Iterate over Utf8 columns and attempt to infer specific types.
    """
    new_exprs = []
    has_changes = False
    
    for name, dtype in df.schema.items():
        if dtype == pl.Utf8:
            new_expr = refine_col_type(df, name)
            # We can't easily check if new_expr is different from original expr object
            # But we can assume we construct a list of expressions
            if new_expr is not None:
                new_exprs.append(new_expr.alias(name))
                # How to know if it changed?
                # We'll just run with_columns. Polars optimizes no-ops.
        else:
            new_exprs.append(pl.col(name))
            
    # We apply all at once
    return df.with_columns(new_exprs)
