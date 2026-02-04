from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class WarehouseSource:
    name: str
    kind: str
    options: dict[str, Any]


def _load_yaml(path: str) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception as e:
        raise RuntimeError("PyYAML is required for warehouse config. Install with: pip install PyYAML") from e
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_sources(config_path: str | None = None) -> dict[str, WarehouseSource]:
    path = config_path or os.environ.get("BLACKBOX_WAREHOUSE_CONFIG", "config/warehouses.yml")
    if not os.path.exists(path):
        return {}
    raw = _load_yaml(path)
    sources = {}
    for name, cfg in (raw.get("sources") or {}).items():
        kind = cfg.get("kind")
        if not kind:
            continue
        sources[name] = WarehouseSource(name=name, kind=kind, options=cfg)
    return sources


def load_dataframe(
    source: WarehouseSource,
    sql: str,
    *,
    params: dict[str, Any] | None = None,
) -> pd.DataFrame:
    kind = source.kind.lower()
    opts = source.options

    if kind == "snowflake":
        try:
            import snowflake.connector  # type: ignore
        except Exception as e:
            raise RuntimeError("snowflake-connector-python required for Snowflake") from e
        conn = snowflake.connector.connect(
            user=opts.get("user") or os.environ.get("SNOWFLAKE_USER"),
            password=opts.get("password") or os.environ.get("SNOWFLAKE_PASSWORD"),
            account=opts.get("account") or os.environ.get("SNOWFLAKE_ACCOUNT"),
            warehouse=opts.get("warehouse") or os.environ.get("SNOWFLAKE_WAREHOUSE"),
            database=opts.get("database") or os.environ.get("SNOWFLAKE_DATABASE"),
            schema=opts.get("schema") or os.environ.get("SNOWFLAKE_SCHEMA"),
            role=opts.get("role") or os.environ.get("SNOWFLAKE_ROLE"),
        )
        try:
            return pd.read_sql(sql, conn, params=params)
        finally:
            conn.close()

    if kind == "bigquery":
        try:
            from google.cloud import bigquery  # type: ignore
        except Exception as e:
            raise RuntimeError("google-cloud-bigquery required for BigQuery") from e
        client = bigquery.Client(project=opts.get("project") or os.environ.get("GOOGLE_CLOUD_PROJECT"))
        job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[]))
        return job.to_dataframe()

    if kind in ("redshift", "postgres", "mysql"):
        try:
            import sqlalchemy as sa  # type: ignore
        except Exception as e:
            raise RuntimeError("sqlalchemy required for SQL warehouses") from e
        url = opts.get("url")
        if not url:
            user = opts.get("user") or os.environ.get("DB_USER")
            password = opts.get("password") or os.environ.get("DB_PASSWORD")
            host = opts.get("host") or os.environ.get("DB_HOST")
            port = opts.get("port") or os.environ.get("DB_PORT")
            database = opts.get("database") or os.environ.get("DB_NAME")
            driver = opts.get("driver")
            scheme = {
                "redshift": "postgresql+psycopg2",
                "postgres": "postgresql+psycopg2",
                "mysql": "mysql+pymysql",
            }[kind]
            auth = f"{user}:{password}@" if user or password else ""
            hostpart = f"{host}:{port}" if port else (host or "")
            url = f"{scheme}://{auth}{hostpart}/{database}"
            if driver:
                url = f"{scheme}+{driver}://{auth}{hostpart}/{database}"
        engine = sa.create_engine(url)
        with engine.connect() as conn:
            return pd.read_sql(sa.text(sql), conn, params=params)

    raise RuntimeError(f"Unsupported warehouse kind: {kind}")
