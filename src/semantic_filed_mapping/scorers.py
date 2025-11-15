from __future__ import annotations

import difflib
from typing import Dict, Iterable, List, Mapping, Tuple

from .normalize import normalize_name, tokenize


def jaccard(a: Iterable[str], b: Iterable[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0


def base_name_scores(src: str, ref: str, synonyms: Mapping[str, List[str]] | None = None) -> Dict[str, float]:
    """Compute multiple similarity signals between two names.

    Returns a dict of component scores:
    - exact: 1.0 if exact string equality (case-sensitive), else 0
    - norm_exact: 0.98 if normalized names equal
    - synonym: 0.97 if src or its normalized/tokens appear in synonyms list for ref
    - jaccard: token set overlap
    - seq: SequenceMatcher ratio on normalized names
    - length_penalty: small penalty for ultra-short tokens
    """
    comp: Dict[str, float] = {
        "exact": 0.0,
        "norm_exact": 0.0,
        "synonym": 0.0,
        "jaccard": 0.0,
        "seq": 0.0,
        "length_penalty": 0.0,
    }

    if src == ref:
        comp["exact"] = 1.0

    src_n = normalize_name(src)
    ref_n = normalize_name(ref)
    if src_n == ref_n:
        comp["norm_exact"] = 0.98

    src_t = tokenize(src_n)
    ref_t = tokenize(ref_n)
    comp["jaccard"] = jaccard(src_t, ref_t)

    comp["seq"] = difflib.SequenceMatcher(a=src_n, b=ref_n).ratio()

    # synonym match: check mapping ref -> list of aliases
    if synonyms:
        aliases = synonyms.get(ref) or synonyms.get(ref_n) or []
        aliases_n = {normalize_name(x) for x in aliases}
        if src in aliases or src_n in aliases_n or any(t in aliases_n for t in src_t):
            comp["synonym"] = 0.97

    # length penalty: discourage mapping from single very short token like 'id' to complex refs
    if len(src_t) == 1 and len(src_t[0]) <= 2 and src_n != ref_n:
        comp["length_penalty"] = -0.02

    return comp


def aggregate_score(components: Mapping[str, float], dtype_boost: float = 0.0, pattern_boost: float = 0.0) -> Tuple[float, Dict[str, float]]:
    """Combine component scores with simple weighted maximum and additive boosts.

    Strategy: take max of [exact, norm_exact, synonym, jaccard, seq], then add boosts and penalty.
    Clamp to [0, 1].
    """
    base_candidates = [
        components.get("exact", 0.0),
        components.get("norm_exact", 0.0),
        components.get("synonym", 0.0),
        components.get("jaccard", 0.0),
        components.get("seq", 0.0),
    ]
    base = max(base_candidates)
    score = base + float(dtype_boost) + float(pattern_boost) + components.get("length_penalty", 0.0)
    score = max(0.0, min(1.0, score))
    details = dict(components)
    details.update({"dtype_boost": dtype_boost, "pattern_boost": pattern_boost, "base": base, "final": score})
    return score, details
