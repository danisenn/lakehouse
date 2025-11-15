import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional, Tuple

import polars as pl


@dataclass
class Dataset:
    name: str
    path: Optional[Path]
    df: pl.DataFrame


class DataSource:
    """
    Abstract data source that yields datasets (name, optional path, DataFrame).
    """

    def iter_datasets(self) -> Iterator[Dataset]:  # pragma: no cover - interface
        raise NotImplementedError


class LocalFilesDataSource(DataSource):
    """
    Scans a local folder recursively and loads CSV/Parquet files with Polars.
    - CSV: tries header=True first, then falls back to header=False and assigns generic names.
    - Parquet: read directly.
    - max_rows: optional row limit per file (0/None = all)
    """

    def __init__(self, root: str | os.PathLike, max_rows: Optional[int] = None) -> None:
        self.root = Path(root)
        self.max_rows = None if not max_rows or max_rows <= 0 else int(max_rows)

    def _read_csv(self, path: Path) -> pl.DataFrame:
        n_rows = self.max_rows
        try:
            return pl.read_csv(path, has_header=True, n_rows=n_rows)
        except Exception:
            df = pl.read_csv(path, has_header=False, n_rows=n_rows)
            # Assign some generic names if none
            cols = [f"column_{i}" for i in range(len(df.columns))]
            rename_map = {old: new for old, new in zip(df.columns, cols)}
            return df.rename(rename_map)

    def _read_parquet(self, path: Path) -> pl.DataFrame:
        df = pl.read_parquet(path)
        if self.max_rows:
            df = df.head(self.max_rows)
        return df

    def iter_datasets(self) -> Iterator[Dataset]:
        exts = {".csv", ".parquet"}
        for p in self.root.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() not in exts:
                continue
            try:
                if p.suffix.lower() == ".csv":
                    df = self._read_csv(p)
                else:
                    df = self._read_parquet(p)
                name = p.relative_to(self.root).as_posix()
                yield Dataset(name=name, path=p, df=df)
            except Exception as e:  # continue on read errors
                name = p.relative_to(self.root).as_posix()
                warn_df = pl.DataFrame({"_error": [str(e)], "_path": [str(p)]})
                yield Dataset(name=name + " (read_error)", path=p, df=warn_df)


class LakehouseSQLDataSource(DataSource):
    """
    Placeholder/stub for a SQL-based lakehouse data source.
    Implement get_connection() and table listing as needed.
    """

    def __init__(self) -> None:
        raise NotImplementedError(
            "LakehouseSQLDataSource is a placeholder. Provide connection details and table iteration logic."
        )
