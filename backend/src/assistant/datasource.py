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
        except pl.ComputeError:
            # Fallback: assume no header if parsing failed (e.g. type mismatch in first row)
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
    SQL-based lakehouse data source using connectorx.
    Supports two modes:
    - query mode: execute a single query
    - schema mode: discover and iterate over all tables in a schema
    """

    def __init__(
        self, 
        connection_uri: str | object, 
        query: str | None = None,
        schema: str | None = None,
        max_rows: int | None = None,
        name: str = "lakehouse_query"
    ) -> None:
        self.connection_uri = connection_uri
        # Normalize empty strings to None for consistency with API/frontend
        self.query = query if (query is not None and str(query).strip() != "") else None
        self.schema = schema if (schema is not None and str(schema).strip() != "") else None
        self.max_rows = max_rows
        self.name = name

    def iter_datasets(self) -> Iterator[Dataset]:
        if self.schema:
            # Schema mode: discover all tables
            yield from self._iter_schema_tables()
        elif self.query:
            # Query mode: single query
            yield from self._iter_single_query()
        else:
            raise ValueError("Either 'query' or 'schema' must be provided")

    def _iter_single_query(self) -> Iterator[Dataset]:
        try:
            q = self.query or ""
            # Apply max_rows in query mode when not already limited
            if self.max_rows and self.max_rows > 0:
                lowered = q.lower()
                if " limit " not in lowered and not lowered.rstrip().endswith(" limit"):
                    q = f"{q.rstrip()} LIMIT {int(self.max_rows)}"
            if hasattr(self.connection_uri, 'toPolars'):
                df = self.connection_uri.toPolars(q)
            else:
                df = pl.read_database(query=q, connection=self.connection_uri)
            yield Dataset(name=self.name, path=None, df=df)
        except Exception as e:
            warn_df = pl.DataFrame({"_error": [str(e)]})
            yield Dataset(name=f"{self.name} (error)", path=None, df=warn_df)

    def _iter_schema_tables(self) -> Iterator[Dataset]:
        # Discover tables from INFORMATION_SCHEMA
        discovery_query = f'SELECT TABLE_NAME FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_SCHEMA = \'{self.schema}\''
        
        try:
            # Check if connection_uri is a Dremio connection object
            if hasattr(self.connection_uri, 'toPolars'):
                # Dremio connection
                tables_df = self.connection_uri.toPolars(discovery_query)
            else:
                # Standard connectorx
                tables_df = pl.read_database(query=discovery_query, connection=self.connection_uri)
            
            table_names = tables_df["TABLE_NAME"].to_list()
            
            for table_name in table_names:
                yield from self._fetch_table(table_name)
                
        except Exception as e:
            warn_df = pl.DataFrame({"_error": [f"Schema discovery failed: {str(e)}"]})
            yield Dataset(name=f"{self.schema} (discovery_error)", path=None, df=warn_df)

    def _fetch_table(self, table_name: str) -> Iterator[Dataset]:
        limit_clause = f" LIMIT {self.max_rows}" if self.max_rows else ""
        query = f'SELECT * FROM {self.schema}."{table_name}"{limit_clause}'
        
        try:
            if hasattr(self.connection_uri, 'toPolars'):
                df = self.connection_uri.toPolars(query)
            else:
                df = pl.read_database(query=query, connection=self.connection_uri)
            
            yield Dataset(name=f"{self.schema}.{table_name}", path=None, df=df)
        except Exception as e:
            warn_df = pl.DataFrame({"_error": [str(e)]})
            yield Dataset(name=f"{self.schema}.{table_name} (error)", path=None, df=warn_df)
