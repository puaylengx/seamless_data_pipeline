# dags/src/dgsi/staging_student/extractors/staging_student_postgresql.py
from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from pathlib import Path


# ------------------------------------------------------------------
# Paths (เหมือน finance invoice)
# ------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]   # .../staging_student/
SQL_DIR  = BASE_DIR / "sql"
SOURCE_SQL_PATH = SQL_DIR / "staging_student.sql"   # ✅ เปลี่ยนเป็นไฟล์ของ staging_student
SOURCE_STATUS_SQL_PATH = SQL_DIR / "status_student.sql"   # ✅ เปลี่ยนเป็นไฟล์ของ staging_student

def load_sql(path: str | Path) -> str:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def build_engine_from_airflow_conn(
    conn,
    driver: str | None = None,
    timeout: int = 5,
    fast_exec: bool = False,
):
    """
    เหมือน finance invoice:
    - รับ Airflow Connection object (BaseHook.get_connection(...))
    - สร้าง SQLAlchemy engine ผ่าน mssql+pyodbc
    - รองรับค่าจาก conn.extra (ถ้ามี)
    """

    extra = getattr(conn, "extra_dejson", None) or {}

    driver  = driver or extra.get("Driver") or "ODBC Driver 18 for SQL Server"
    encrypt = str(extra.get("Encrypt", "no")).lower()
    trust   = str(extra.get("TrustServerCertificate", "yes")).lower()
    timeout = int(extra.get("Connection Timeout", timeout) or timeout)

    # host + port
    server = conn.host if not conn.port else f"{conn.host},{conn.port}"
    database = conn.schema

    odbc = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={database};"
        f"UID={conn.login};PWD={conn.password};"
        f"Encrypt={encrypt};TrustServerCertificate={trust};"
        f"Connection Timeout={timeout};"
    )

    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}",
        pool_pre_ping=True,
        fast_executemany=bool(fast_exec),
    )


def extract_staging_student(src_conn, sql_path: str | Path = SOURCE_SQL_PATH) -> pd.DataFrame:
    """
    - build engine จาก Airflow conn
    - อ่าน SQL จากไฟล์
    - pd.read_sql
    """
    engine = build_engine_from_airflow_conn(src_conn)
    query = load_sql(sql_path)
    return pd.read_sql(query, engine)


def extract_status_dataframe(src_conn, sql_path: str | Path = SOURCE_STATUS_SQL_PATH) -> pd.DataFrame:
    engine = build_engine_from_airflow_conn(src_conn)
    query = load_sql(sql_path)
    return pd.read_sql(query, engine)
