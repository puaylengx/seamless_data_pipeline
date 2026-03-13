# dags/src/config.py
from __future__ import annotations
from airflow.models import Variable
from pathlib import Path

# ---- Airflow Variables (มี default เผื่อยังไม่ได้ตั้งใน UI) ----
GCP_PROJECT      = Variable.get("BQ_PROJECT", default_var="muic-data-prod")
BQ_DATASET       = Variable.get("BQ_DATASET", default_var="Education")
BQ_TABLE         = Variable.get("BQ_TABLE",   default_var="student_information")
MSSQL_CONN_ID    = Variable.get("MSSQL_CONN_ID", default_var="mssql_student_info")
GCP_CONN_ID      = Variable.get("GCP_CONN_ID",   default_var="google_cloud_default")
SQL_PATH         = Variable.get(
    "STUDENT_INFO_SQL_PATH",
    default_var="/opt/airflow/dags/src/education/student_information/sql/student_information.sql"
)

DATA_DIR         = Path(
    Variable.get("STUDENT_INFO_DATA_DIR", default_var="/opt/airflow/dags/data/student_information")
)

# สร้างโฟลเดอร์ปลายทางการทำงาน (extract/transform) หากยังไม่มี
(DATA_DIR / "extract").mkdir(parents=True, exist_ok=True)
(DATA_DIR / "transform").mkdir(parents=True, exist_ok=True)
