# dags/student_information_dag.py
from __future__ import annotations
import pendulum
from pathlib import Path
import pandas as pd
import logging

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
from _callbacks import on_task_failure_callback, on_task_success_callback, on_dag_success_callback
from utils_recipients import resolve_recipients

# logger = get_logger("student_information_dag")
logger = logging.getLogger("airflow.task")
TZ = "Asia/Bangkok"

# ✅ อ่านผู้รับจาก ENV → ถ้าไม่มี → จาก Variable → ถ้าไม่มี → ใช้ fallback
DEFAULT_RECIPIENTS = resolve_recipients()
logger.info("Summary recipients: %s", DEFAULT_RECIPIENTS)

default_args = {
    "owner": "data-eng",
    "retries": 1,
}

@dag(
    dag_id="student_information_pipeline",
    start_date=pendulum.datetime(2026, 3, 1, tz=TZ),
    schedule="@daily",
    catchup=False,
    # default_args=default_args,
    default_args={
        "on_failure_callback": on_task_failure_callback,
        "on_success_callback": on_task_success_callback,
        "email_on_success": False,
        "email_on_failure": False,
    },
    on_success_callback=on_dag_success_callback,
    params={  # <<< กำหนดค่ามาตรฐาน (แก้ได้ตอน Trigger)
        "statuses": ["dm","ex","g","la","np","prc","pa","rs","s"],
        "min_academic_year": 2016,
        "write_disposition": "WRITE_TRUNCATE",
    },
    tags=["education", "student_information"],
)
def student_information_pipeline():
    @task(
        retries=1,
        on_success_callback=on_task_success_callback  # ถ้าต้องการอีเมลเมื่อสำเร็จ
    )
    def extract() -> dict:
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

        msg = f"Extracted rows: {len(df)} → {out_path}"
        logger.info(msg)
        return {"rows": int(len(df)), "path": str(out_path), "message": msg}

    @task(
        retries=1,
        on_success_callback=on_task_success_callback  # ถ้าต้องการอีเมลเมื่อสำเร็จ
    )
    def transform(in_obj: dict) -> dict:
        in_path = in_obj["path"]
        df = pd.read_parquet(in_path)
        df_t = transform_student_information(df)

        out_path = Path(in_path.replace("/extract/", "/transform/").replace("student_info_", "student_info_t_"))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df_t.to_parquet(out_path, index=False)

        msg = f"Transformed rows: {len(df_t)} → {out_path}"
        logger.info(msg)
        return {"rows": int(len(df_t)), "path": str(out_path), "message": msg}


    @task(
        retries=1,
        on_success_callback=on_task_success_callback  # ถ้าต้องการอีเมลเมื่อสำเร็จ
    )
    def validate(in_obj: dict) -> dict:
        in_path = in_obj["path"]
        df = pd.read_parquet(in_path)
        validate_student_information(df)

        msg = f"Validation passed for {len(df)} records"
        logger.info(msg)
        return {"rows": int(len(df)), "path": in_path, "message": msg}


    @task(
        retries=1,
        on_success_callback=on_task_success_callback  # ถ้าต้องการอีเมลเมื่อสำเร็จ
    )
    def load(in_obj: dict, write_disposition: str = "WRITE_TRUNCATE") -> dict:
        import pendulum
        df = pd.read_parquet(in_obj["path"])

        if "ingestion_date" not in df.columns:
            df["ingestion_date"] = pd.to_datetime(pendulum.now("Asia/Bangkok").date())

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

        msg = f"Loaded to BigQuery {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE} ({len(df)} rows)"
        logger.info(msg)
        return {"rows": int(len(df)), "path": in_obj["path"], "message": msg}


    # ---- Orchestration ----
    p1 = extract()
    p2 = transform(p1)
    p3 = validate(p2)
    # load(p3, write_disposition="{{ params.write_disposition }}")
    load(p3)

student_information_dag = student_information_pipeline()