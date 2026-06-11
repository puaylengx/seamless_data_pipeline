# dags/src/config.py
from __future__ import annotations
from airflow.models import Variable
from pathlib import Path
from dataclasses import dataclass
from airflow.models import Variable
from airflow.hooks.base import BaseHook


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

# for printer
PRINTER_BQ_DATASET       = Variable.get("PRINTER_BQ_DATASET", default_var="PaperCut")
PRINTER_BQ_TABLE         = Variable.get("PRINTER_BQ_TABLE",   default_var="monthly_printer_usage")
PRINTER_POSTGRESQL_CONN_ID    = Variable.get("PRINTER_POSTGRESQL_CONN_ID", default_var="mssql_student_info")
PRINTER_SQL_PATH         = Variable.get(
    "PRINTER_SQL_PATH",
    default_var="/opt/airflow/dags/src/information_tech/printer_information/sql/printer_solution.sql"
)

# สร้างโฟลเดอร์ปลายทางการทำงาน (extract/transform) หากยังไม่มี
(DATA_DIR / "extract").mkdir(parents=True, exist_ok=True)
(DATA_DIR / "transform").mkdir(parents=True, exist_ok=True)

@dataclass
class JobConfig:
    target_schema: str = "dbo"
    target_table: str = "finance_invoice_20251110"
    key_col: str = "invoiceId"
    batch_size: int = 1000

@dataclass
class PathConfig:
    # ที่เก็บไฟล์ระหว่าง task (ควรเป็น shared volume ของ worker)
    workdir: str = "/opt/airflow/data/finance_invoice"

def get_conn(conn_id: str):
    # ดึง Connection จาก Airflow
    return BaseHook.get_connection(conn_id)

def get_job_config() -> JobConfig:
    # ตั้งค่า override ได้ผ่าน Airflow Variable
    return JobConfig(
        target_schema=Variable.get("FIN_INV_TARGET_SCHEMA", default_var="dbo"),
        target_table=Variable.get("FIN_INV_TARGET_TABLE", default_var="finance_invoice_20251110"),
        key_col=Variable.get("FIN_INV_KEY_COL", default_var="invoiceId"),
        batch_size=int(Variable.get("FIN_INV_BATCH_SIZE", default_var="1000")),
    )

@dataclass
class JobConfigStaging:
    target_schema: str = "dbo"
    target_table: str = "StagingStudent_20251105"
    key_col: str = "studentcode"
    batch_size: int = 1000
    
def get_job_config_staging() -> JobConfigStaging:
    # ตั้งค่า override ได้ผ่าน Airflow Variable
    return JobConfigStaging(
        target_schema=Variable.get("STAGING_STD_TARGET_SCHEMA", default_var="dbo"),
        target_table=Variable.get("STAGING_STD_TARGET_TABLE", default_var="StagingStudent_20251105"),
        key_col=Variable.get("STAGING_STD_KEY_COL", default_var="studentcode"),
        batch_size=int(Variable.get("STAGING_STD_BATCH_SIZE", default_var="1000")),
    )


def get_staging_student_excel_path() -> str:
    """
    Path ของไฟล์ Excel ที่มีข้อมูล talent + extra (numberOfSiblings ฯลฯ)
    ตั้งค่าได้ผ่าน Airflow Variable: STAGING_STD_EXCEL_PATH
    ถ้าไม่ได้ตั้ง จะคืนค่าว่าง → transform จะข้ามการ merge Excel
    """
    return Variable.get(
        "STAGING_STD_EXCEL_PATH",
        default_var="/opt/airflow/dags/data/staging_student/Candidate.xlsx",
    )


