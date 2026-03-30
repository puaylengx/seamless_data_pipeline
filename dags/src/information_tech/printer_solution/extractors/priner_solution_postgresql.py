from __future__ import annotations

import pandas as pd
from pathlib import Path
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Connection as SAConnection

from airflow.hooks.base import BaseHook


BASE_DIR = Path(__file__).resolve().parents[1]
SQL_DIR = BASE_DIR / "sql"
SOURCE_SQL_PATH = SQL_DIR / "printer_solution.sql"


def load_sql(path: str | Path) -> str:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found at {path}")
    return path.read_text(encoding="utf-8")


def _looks_like_airflow_connection(obj) -> bool:
    """
    รองรับทั้ง:
    - airflow.models.Connection
    - airflow.sdk.definitions.connection.Connection
    """
    required_attrs = ("login", "password", "host", "port", "schema")
    return all(hasattr(obj, attr) for attr in required_attrs)


def build_engine_from_airflow_like_conn(conn) -> Engine:
    if not conn.schema:
        raise ValueError("Airflow connection schema (database) is empty")

    url = (
        "postgresql+psycopg2://"
        f"{quote_plus(conn.login)}:{quote_plus(conn.password)}"
        f"@{conn.host}:{conn.port}/{conn.schema}"
    )

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={
            "connect_timeout": 5,
            "sslmode": "disable",
        },
    )


def fetch_printer_solution_postgresql(src_conn) -> pd.DataFrame:
    """
    รองรับ src_conn ได้หลายแบบ:
    - str → Airflow conn_id
    - Airflow SDK Connection
    - airflow.models.Connection
    - SQLAlchemy Engine
    - SQLAlchemy Connection
    """

    # ✅ case 1: conn_id
    if isinstance(src_conn, str):
        af_conn = BaseHook.get_connection(src_conn)
        engine = build_engine_from_airflow_like_conn(af_conn)

    # ✅ case 2: Airflow Connection (SDK หรือ legacy)
    elif _looks_like_airflow_connection(src_conn):
        engine = build_engine_from_airflow_like_conn(src_conn)

    # ✅ case 3: SQLAlchemy Engine
    elif isinstance(src_conn, Engine):
        engine = src_conn

    # ✅ case 4: SQLAlchemy Connection
    elif isinstance(src_conn, SAConnection):
        engine = src_conn.engine

    else:
        raise TypeError(
            f"Unsupported src_conn type: {type(src_conn)}. "
            "Expected conn_id, Airflow Connection (SDK/legacy), or SQLAlchemy engine."
        )

    query = load_sql(SOURCE_SQL_PATH)

    with engine.connect() as connection:
        df = pd.read_sql(query, connection)

    return df
