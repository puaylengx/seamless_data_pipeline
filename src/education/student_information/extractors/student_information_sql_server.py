import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from config import PipelineConfig

def fetch_student_info(cfg: PipelineConfig) -> pd.DataFrame:
    # ใช้ SQL ในไฟล์ .sql แยกต่างหาก
    base_sql = cfg.query_path.read_text(encoding="utf-8")
    
    # add where condition แบบ parameterized
    # create placeholder for statuses: :s0, :s1, ...
    status_placeholder = ", ".join([f":s{i}" for i, _ in enumerate(cfg.allowed_statuses)])
    where_clause = f"WHERE academic_year >= :min_year AND status IN ({status_placeholder})"
    
    sql = text(base_sql + where_clause)
    
    params = {"min_year": cfg.min_academic_year}
    params.update({f"s{i}": v for i, v in enumerate(cfg.allowed_statuses)})
    
    # ทำ connection string สำหรับ SQL Server ODBC driver 18
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={cfg.sky_host};"
        f"DATABASE={cfg.sky_database}"
        f"UID={cfg.sky_username};"
        f"PWD={cfg.sky_password};"
        "TrustServerCertificate=yes;"
    )
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
    engine = create_engine(connection_url, fast_executemany=True)
    
    with engine.begin() as conn:
        df = pd.read_sql(sql, conn)
    
    return df