from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import polars as pl

import json
from pathlib import Path
from .normalize import normalize_name
from .scorers import base_name_scores, aggregate_score

# Load default synonyms
SYNONYMS_FILE = Path(__file__).parent / "synonyms.json"
DEFAULT_SYNONYMS = {}
if SYNONYMS_FILE.exists():
    try:
        with open(SYNONYMS_FILE, "r", encoding="utf-8") as f:
            DEFAULT_SYNONYMS = json.load(f)
    except Exception:
        pass


@dataclass
class Candidate:
    ref: str
    score: float
    details: Dict[str, float]


ResultMapping = Dict[str, Dict[str, Any]]


class SemanticFieldMapper:
    """
    Map dataset columns to a set of reference fields using normalization, token overlap,
    fuzzy ratio, basic synonym lists, and simple value-pattern hints.

    Parameters
    ----------
    reference_fields: list of canonical field names.
    synonyms: mapping from reference field -> list of alias names.
    threshold: minimal score to accept a mapping.
    epsilon: ambiguity window. If the 2nd best candidate is within `epsilon` of the best,
             mark the column as ambiguous.
    strategy: 'auto'|'exact'|'normalized'|'fuzz 1y' — currently advisory; 'auto' uses all.
    force: if True, accept best above threshold even if ambiguous.
    """

    def __init__(
        self,
        reference_fields: Sequence[str],
        synonyms: Optional[Mapping[str, Sequence[str]]] = None,
        threshold: float = 0.72,
        epsilon: float = 0.03,
        strategy: str = "auto",
        force: bool = False,
    ) -> None:
        self.reference_fields: List[str] = list(reference_fields)
        # normalize synonym keys but keep original refs list
        self.synonyms: Dict[str, List[str]] = {}
        
        # Merge provided synonyms with defaults
        combined_synonyms = DEFAULT_SYNONYMS.copy()
        if synonyms:
            for k, vals in synonyms.items():
                if k in combined_synonyms:
                    combined_synonyms[k].extend(vals)
                else:
                    combined_synonyms[k] = list(vals)
        
        self.synonyms = combined_synonyms
        self.threshold = float(threshold)
        self.epsilon = float(epsilon)
        self.strategy = strategy
        self.force = force

    def _scores_for_column(
        self,
        col_name: str,
        series: Optional["pl.Series"] = None,
    ) -> List[Candidate]:
        # pattern hints/boosts from values
        dtype_boost = 0.0
        pattern_boost = 0.0
        detected_type = None
        
        if series is not None and pl is not None:
            try:
                # Use SemanticTypeDetector for strong signals
                from src.schema_recognition.inference.semantic import SemanticTypeDetector
                detector = SemanticTypeDetector()
                # We only need to check this specific column
                # Create a temporary DataFrame for the detector
                temp_df = pl.DataFrame({col_name: series})
                detected_types = detector.detect(temp_df)
                detected_type = detected_types.get(col_name)
                
                # dtype boosts: numeric/string hints
                dt = getattr(series, "dtype", None)
                if dt in (getattr(pl, "Int8", None), getattr(pl, "Int16", None), getattr(pl, "Int32", None), getattr(pl, "Int64", None),
                          getattr(pl, "UInt8", None), getattr(pl, "UInt16", None), getattr(pl, "UInt32", None), getattr(pl, "UInt64", None),
                          getattr(pl, "Float32", None), getattr(pl, "Float64", None)):
                    dtype_boost = 0.02
            except Exception:
                pass

        candidates: List[Candidate] = []
        for ref in self.reference_fields:
            # Check for content-based match
            # If the detected semantic type matches the reference field (or its synonyms), give a massive boost
            current_pattern_boost = 0.0
            if detected_type:
                # Normalize ref and detected type for comparison
                ref_n = normalize_name(ref)
                type_n = normalize_name(detected_type)
                
                # Direct match (e.g. ref="email", type="Email")
                if ref_n == type_n:
                    current_pattern_boost = 0.5
                else:
                    # Check synonyms (e.g. ref="contact", synonyms=["email", ...], type="Email")
                    aliases = self.synonyms.get(ref) or self.synonyms.get(ref_n) or []
                    aliases_n = {normalize_name(x) for x in aliases}
                    if type_n in aliases_n:
                        current_pattern_boost = 0.5

            comps = base_name_scores(col_name, ref, synonyms=self.synonyms)
            score, details = aggregate_score(comps, dtype_boost=dtype_boost, pattern_boost=current_pattern_boost)
            candidates.append(Candidate(ref=ref, score=score, details=details))
        # sort desc by score
        candidates.sort(key=lambda c: c.score, reverse=True)
        return candidates

    def map_columns(
        self,
        df_or_columns: Union["pl.DataFrame", Sequence[str]],
    ) -> Dict[str, Any]:
        """
        Compute mapping for dataset columns.

        Returns dict with keys:
        - mapping: {src_col: {target, score, reason(details)}}
        - ambiguous: {src_col: [ {target, score}, ... ]}
        - unmapped: [src_col]
        - scores: {src_col: {ref: score}}
        """
        if pl is not None and isinstance(df_or_columns, pl.DataFrame):  # type: ignore
            df = df_or_columns
            columns = list(df.columns)
        else:
            df = None
            columns = [str(c) for c in df_or_columns]  # type: ignore
        result: Dict[str, Any] = {
            "mapping": {},
            "ambiguous": {},
            "unmapped": [],
            "scores": {},
        }
        for col in columns:
            series = df[col] if (df is not None and col in df.columns) else None
            cands = self._scores_for_column(col, series=series)
            result["scores"][col] = {c.ref: c.score for c in cands}
            if not cands:
                result["unmapped"].append(col)
                continue
            best = cands[0]
            second = cands[1] if len(cands) > 1 else None
            # acceptance check
            accepted = best.score >= self.threshold
            ambiguous = False
            if second is not None and (best.score - second.score) <= self.epsilon:
                ambiguous = True
            if not accepted:
                result["unmapped"].append(col)
                continue
            if ambiguous and not self.force:
                # Check if ambiguous candidates are effectively the same (e.g. 'EMAIL' vs 'email')
                # If so, resolve ambiguity by picking the best one.
                if best.ref.lower() == second.ref.lower():
                    ambiguous = False
                else:
                    result["ambiguous"][col] = [
                        {"target": c.ref, "score": c.score} for c in cands[: min(5, len(cands))]
                    ]
                    continue
            result["mapping"][col] = {
                "target": best.ref,
                "score": best.score,
                "reason": best.details,
            }
        return result


def map_columns(
    df: Optional["pl.DataFrame"] = None,
    columns: Optional[Sequence[str]] = None,
    reference_fields: Sequence[str] = (),
    synonyms: Optional[Mapping[str, Sequence[str]]] = None,
    threshold: float = 0.72,
    epsilon: float = 0.03,
    strategy: str = "auto",
    force: bool = False,
) -> Dict[str, Any]:
    """Convenience wrapper around SemanticFieldMapper.map_columns.

    Provide either a DataFrame `df` or a sequence of column names via `columns`.
    """
    if df is None and not columns:
        raise ValueError("Provide either a Polars DataFrame via df or a list of column names via columns")
    mapper = SemanticFieldMapper(
        reference_fields=reference_fields,
        synonyms=synonyms,
        threshold=threshold,
        epsilon=epsilon,
        strategy=strategy,
        force=force,
    )
    df_or_cols: Union["pl.DataFrame", Sequence[str]] = df if df is not None else list(columns or [])
    return mapper.map_columns(df_or_cols)
