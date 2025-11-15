import re
from typing import Iterable, List, Set

from src.utils.cleaning import clean_column_name


_WORD_RE = re.compile(r"[A-Za-z0-9]+")
_DEFAULT_STOPWORDS: Set[str] = {
    "id", "ids", "no", "num", "nr", "code", "cd", "key",
    "flag", "is", "has", "at", "dt", "ts", "ref", "uid"
}


def normalize_name(name: str) -> str:
    """Normalize a column/field name using project cleaning utility and extra rules.

    - lowercase
    - replace spaces with underscores (handled by clean_column_name)
    - collapse multiple underscores
    - strip leading/trailing underscores
    """
    base = clean_column_name(name)
    base = re.sub(r"[^a-z0-9_]+", "_", base)
    base = re.sub(r"_+", "_", base).strip("_")
    return base


def tokenize(name: str, stopwords: Iterable[str] | None = None) -> List[str]:
    """Tokenize a name into alphanumeric words, minus common stopwords."""
    s = normalize_name(name)
    tokens = [m.group(0) for m in _WORD_RE.finditer(s)]
    sw: Set[str] = set(stopwords) if stopwords is not None else _DEFAULT_STOPWORDS
    return [t for t in tokens if t not in sw]
