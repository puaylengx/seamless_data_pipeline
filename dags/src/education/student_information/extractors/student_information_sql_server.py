# src/education/student_information/extractors/student_information_sql_server.py
from __future__ import annotations
from pathlib import Path
import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from src.helpers.logger import get_logger

logger = get_logger(__name__)

def load_sql(path: str | Path) -> str:
    return Path(path).read_text(encoding="utf-8")

def fetch_student_information(
    mssql_conn_id: str,
    sql_path: str | Path,
) -> pd.DataFrame:
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    sql = load_sql(sql_path)
    return hook.get_pandas_df(sql=sql)