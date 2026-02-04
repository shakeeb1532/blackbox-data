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
      - DuckDB relations via to_df()
    """
    if isinstance(obj, pd.DataFrame):
        return obj
    if hasattr(obj, "toPandas"):
        return obj.toPandas()
    if hasattr(obj, "to_pandas"):
        return obj.to_pandas()
    if hasattr(obj, "to_df"):
        return obj.to_df()
    raise TypeError(f"Unsupported dataframe type: {describe_engine(obj)}")


def is_dataframe_like(obj: Any) -> bool:
    if isinstance(obj, pd.DataFrame):
        return True
    return any(hasattr(obj, attr) for attr in ("toPandas", "to_pandas", "to_df"))
