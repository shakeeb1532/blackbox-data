from __future__ import annotations

from typing import Any

import pandas as pd


def describe_engine(obj: Any) -> str:
    if obj is None:
        return "none"
    t = type(obj)
    mod = getattr(t, "__module__", "")
    name = getattr(t, "__name__", str(t))
    return f"{mod}.{name}".strip(".")


def to_pandas(obj: Any) -> pd.DataFrame:
    """
    Convert common dataframe-like objects to pandas DataFrame.
    Supports:
      - pandas.DataFrame (no-op)
      - Spark DataFrame via toPandas()
      - Polars via to_pandas()
      - Polars LazyFrame via collect().to_pandas()
      - PyArrow Table/RecordBatch/Dataset
      - DuckDB relations via to_df()
    """
    if isinstance(obj, pd.DataFrame):
        return obj
    # Polars LazyFrame
    if hasattr(obj, "collect") and obj.__class__.__module__.startswith("polars"):
        obj = obj.collect()
    if hasattr(obj, "toPandas"):
        return obj.toPandas()
    if hasattr(obj, "to_pandas"):
        return obj.to_pandas()
    if hasattr(obj, "to_df"):
        return obj.to_df()
    # PyArrow
    try:
        import pyarrow as pa  # type: ignore
        if isinstance(obj, pa.Table):
            return obj.to_pandas()
        if isinstance(obj, pa.RecordBatch):
            return pa.Table.from_batches([obj]).to_pandas()
        if isinstance(obj, pa.dataset.Dataset):
            return obj.to_table().to_pandas()
    except Exception:
        pass
    raise TypeError(f"Unsupported dataframe type: {describe_engine(obj)}")


def is_dataframe_like(obj: Any) -> bool:
    if isinstance(obj, pd.DataFrame):
        return True
    if hasattr(obj, "collect") and obj.__class__.__module__.startswith("polars"):
        return True
    try:
        import pyarrow as pa  # type: ignore
        if isinstance(obj, (pa.Table, pa.RecordBatch, pa.dataset.Dataset)):
            return True
    except Exception:
        pass
    return any(hasattr(obj, attr) for attr in ("toPandas", "to_pandas", "to_df"))


def duckdb_sql_to_pandas(conn: Any, sql: str) -> pd.DataFrame:
    """
    Execute SQL against a DuckDB connection and return a pandas DataFrame.
    Supports conn.sql(...) or conn.execute(...).
    """
    if hasattr(conn, "sql"):
        rel = conn.sql(sql)
    elif hasattr(conn, "execute"):
        rel = conn.execute(sql)
    else:
        raise TypeError("DuckDB connection must support .sql() or .execute()")

    if hasattr(rel, "df"):
        return rel.df()
    if hasattr(rel, "fetchdf"):
        return rel.fetchdf()
    if hasattr(rel, "to_df"):
        return rel.to_df()
    raise TypeError("DuckDB relation result does not support df/fetchdf/to_df")
