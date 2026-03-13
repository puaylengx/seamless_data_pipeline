# dags/student_information_dag.py
from __future__ import annotations
import pendulum
from pathlib import Path
import pandas as pd

from airflow.sdk import dag, task, get_current_context

# ---- Import โมดูลของเรา ----
from src.config import (
    MSSQL_CONN_ID, GCP_CONN_ID, GCP_PROJECT, BQ_DATASET, BQ_TABLE, SQL_PATH, DATA_DIR
)
from src.education.student_information.extractors.student_information_sql_server import fetch_student_information
from src.education.student_information.transformers.student_information import transform_student_information
from src.education.student_information.validators.student_information import validate_student_information
from src.education.student_information.loaders.student_information_bigquery import load_dataframe_to_bq
from src.education.student_information.helpers.logger import get_logger


# logger = get_logger("student_information_dag")
logger = get_logger()
TZ = "Asia/Bangkok"

default_args = {
    "owner": "data-eng",
    "retries": 1,
}

@dag(
    dag_id="student_information_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz=TZ),
    catchup=False,
    default_args=default_args,
    params={  # <<< กำหนดค่ามาตรฐาน (แก้ได้ตอน Trigger)
        "statuses": ["dm","ex","g","la","np","prc","pa","rs","s"],
        "min_academic_year": 2016,
        "write_disposition": "WRITE_TRUNCATE",
    },
    tags=["education", "student_information"],
)
def student_information_pipeline():
    @task()
    def extract() -> str:
        """
        ดึงข้อมูลจาก SQL Server แล้วบันทึกเป็น Parquet
        return: path ของไฟล์ parquet
        """
        # อ่าน params จาก context
        ctx = get_current_context()
        p = ctx["params"]
        statuses = p.get("statuses", [])
        min_year = int(p.get("min_academic_year", 2016))
        
        df = fetch_student_information(
            mssql_conn_id=MSSQL_CONN_ID,
            sql_path=SQL_PATH,
            statuses=statuses,
            min_year=min_year,
        )
        
        run_id = ctx["run_id"].replace("|", "_").replace(":", "_")
        out_path = DATA_DIR / "extract" / f"student_info_{run_id}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, index=False)
        logger.info("Extracted rows: %s → %s", len(df), out_path)
        return str(out_path)

    @task()
    def transform(in_path: str) -> str:
        """
        อ่าน Parquet ที่ extract แล้ว transform และเขียนเป็น Parquet ใหม่
        return: path ของไฟล์ parquet ใหม่
        """
        df = pd.read_parquet(in_path)
        df_t = transform_student_information(df)
        out_path = Path(in_path.replace("/extract/", "/transform/").replace("student_info_", "student_info_t_"))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df_t.to_parquet(out_path, index=False)
        logger.info("Transformed rows: %s → %s", len(df_t), out_path)
        return str(out_path)

    @task()
    def validate(in_path: str) -> str:
        df = pd.read_parquet(in_path)
        validate_student_information(df)
        logger.info("Validation passed for %s records", len(df))
        return in_path  # ส่งต่อ path เดิมถ้าผ่าน
    

    @task()
    def load(in_path: str, write_disposition: str = "WRITE_TRUNCATE") -> None:
        import pandas as pd
        import pendulum
        df = pd.read_parquet(in_path)

        # --- ให้แน่ใจว่ามี 'ingestion_date' สำหรับ table ที่ partition ด้วย field นี้ ---
        if "ingestion_date" not in df.columns:
            # ใช้วันรันเป็นค่า ingestion_date (หรือเลือก field วันที่ในข้อมูลจริง ถ้ามี)
            df["ingestion_date"] = pd.to_datetime(pendulum.now("Asia/Bangkok").date())

        # --- (ถ้า “ตารางเดิม” ยังมี clustering ที่อ้าง column 'code'
        #      แต่ใน pipeline เรารีเนมเป็น 'student_id') ให้เติม 'code' กลับมาเพื่อให้ clustering ทำงานได้เต็มที่ ---
        if "code" not in df.columns and "student_id" in df.columns:
            df["code"] = df["student_id"]

        load_dataframe_to_bq(
            df=df,
            project_id=GCP_PROJECT,
            dataset=BQ_DATASET,
            table=BQ_TABLE,
            gcp_conn_id=GCP_CONN_ID,
            write_disposition=write_disposition,
            location="asia-southeast1",
        )
        
        # ✅ บันทึกผลลัพธ์ด้วย task logger (จะขึ้น prefix เป็น INFO ถูกต้อง)
        logger.info("Loaded to BigQuery %s.%s.%s (%s rows)",GCP_PROJECT, BQ_DATASET, BQ_TABLE, len(df))

    # ---- Orchestration ----
    p1 = extract()
    p2 = transform(p1)
    p3 = validate(p2)
    # load(p3, write_disposition="{{ params.write_disposition }}")
    load(p3)

student_information_dag = student_information_pipeline()