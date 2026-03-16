
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SQL_DIR  = BASE_DIR / "sql"
SOURCE_SQL_PATH = SQL_DIR / "finance_invoice.sql"

def load_sql(path: str | Path) -> str:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")

def build_engine_from_airflow_conn(conn, driver="ODBC Driver 18 for SQL Server", timeout=5):
    # conn.host, conn.schema, conn.login, conn.password, conn.port
    server = conn.host if not conn.port else f"{conn.host},{conn.port}"
    database = conn.schema

    odbc = (
        f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
        f"UID={conn.login};PWD={conn.password};"
        f"Encrypt=no;TrustServerCertificate=yes;Connection Timeout={timeout};"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}", pool_pre_ping=True)

def extract_invoices(src_conn) -> pd.DataFrame:
    engine = build_engine_from_airflow_conn(src_conn)
    query = load_sql(SOURCE_SQL_PATH)
    return pd.read_sql(query, engine)

