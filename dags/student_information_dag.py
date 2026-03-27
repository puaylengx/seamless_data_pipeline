# dags/student_information_dag.py
from __future__ import annotations

import math
import os
import time
from pathlib import Path

import pendulum
import pandas as pd
import logging

from airflow.decorators import dag, task
from airflow.sdk import get_current_context
from airflow.exceptions import AirflowFailException

# ---- Import โมดูลของเรา ----
from src.config import (
    MSSQL_CONN_ID, GCP_CONN_ID, GCP_PROJECT, BQ_DATASET, BQ_TABLE, SQL_PATH, DATA_DIR
)

# Pipeline modules
from src.education.student_information.extractors.student_information_sql_server import fetch_student_information
from src.education.student_information.transformers.student_information import transform_student_information
from src.education.student_information.validators.student_information import validate_student_information
from src.education.student_information.loaders.student_information_bigquery import load_dataframe_to_bq_upsert_with_counts

# helper กลาง
from src.helpers.audit import write_audit_line
from src.helpers.emailer import load_email_config_from_env, send_summary_email

logger = logging.getLogger("airflow.task")
TZ = "Asia/Bangkok"

@dag(
    dag_id="student_information_pipeline",
    start_date=pendulum.datetime(2026, 3, 1, tz=TZ),
    # schedule="@daily",
    schedule="0 5 * * *", # นาที  ชั่วโมง  วันของเดือน  เดือน  วันของสัปดาห์
    catchup=False,
    tags=["education", "student_information", "etl", "standard"],
    default_args={"retries": 2},
    params={
        "write_disposition": "WRITE_TRUNCATE",
    },
)
def student_information_etl():
    @task()
    def mark_start() -> float:
        return time.time()
    
    @task()
    def extract_to_file() -> str:
        """
        ดึงข้อมูลจาก SQL Server แล้วบันทึกเป็น Parquet
        return: path ของไฟล์ parquet
        """
        # อ่าน params จาก context
        ctx = get_current_context()
        p = ctx["params"]
        
        df = fetch_student_information(
            mssql_conn_id=MSSQL_CONN_ID,
            sql_path=SQL_PATH,
        )
        
        run_id = ctx["run_id"].replace("|", "_").replace(":", "_")
        out_path = DATA_DIR / "extract" / f"student_info_{run_id}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_parquet(out_path, index=False)
        logger.info("✅ Extracted %s rows → %s", len(df), out_path)
        
        return str(out_path)

    @task()
    def transform_file(raw_path: str) -> str:
        df = pd.read_parquet(raw_path)
        if df.empty:
            # ส่งไฟล์ว่างต่อไปได้
            out_path = Path(raw_path).as_posix().replace("/extract/", "/transform/").replace("student_info_", "student_info_t_")
            out_path = Path(out_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(out_path, index=False)
            logger.info("⚠️ Transform skipped (no rows) → %s", out_path)
            return str(out_path)

        df_t = transform_student_information(df)

        out_path = Path(raw_path).as_posix().replace("/extract/", "/transform/").replace("student_info_", "student_info_t_")
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        df_t.to_parquet(out_path, index=False)
        logger.info("✅ Transformed %s rows → %s", len(df_t), out_path)

        return str(out_path)

    @task()
    def validate_file(clean_path: str) -> dict:
        df = pd.read_parquet(clean_path)

        # กรณีไม่มีข้อมูล: ไม่ fail แต่ส่ง report กลับ
        if df.empty:
            report = {"ok": True, "rows": 0, "path": clean_path, "issues": []}
            logger.info("⚠️ Validation: no rows (ok) → %s", clean_path)
            return report

        # ถ้า validate_student_information โยน exception ให้ถือว่า fail
        validate_student_information(df)

        report = {"ok": True, "rows": int(len(df)), "path": clean_path, "issues": []}
        logger.info("✅ Validation passed for %s records", len(df))
        return report

    @task()
    def load(clean_path: str, report: dict, start_ts: float) -> dict:    
        ctx = get_current_context()
        p = ctx.get("params", {})
        write_disposition = p.get("write_disposition", "WRITE_TRUNCATE")

        run_date = pendulum.now(TZ).format("YYYY-MM-DD HH:mm:ss")

        # no_data branch: return schema มาตรฐานให้ email ทำงานได้
        if not report.get("ok"):
            raise AirflowFailException(f"Validation failed: {report}")

        df = pd.read_parquet(clean_path)
        rows_total = int(len(df))

        if rows_total == 0:
            duration_sec = round(time.time() - start_ts, 2)
            result = {
                "subject": "Education : Student Information",
                "status": "no_data",
                "inserted": 0,
                "updated": 0,
                "duration_sec": duration_sec,
                "rows_total": 0,
                "batches_total": 0,
                "updated_samples": [],
                "target_table": f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
                "run_date": run_date,
            }
            # audit summary (optional)
            write_audit_line({"ts": run_date, "pipeline": "student_information", "action": "SUMMARY", **result})
            return result

        # เพิ่ม ingestion_date ตามของเดิม
        if "ingestion_date" not in df.columns:
            df["ingestion_date"] = pd.to_datetime(pendulum.now(TZ).date())

        # normalize code column
        if "code" not in df.columns and "student_id" in df.columns:
            df["code"] = df["student_id"]

        # โหลดเข้า BigQuery
        # load_dataframe_to_bq(
        #     df=df,
        #     project_id=GCP_PROJECT,
        #     dataset=BQ_DATASET,
        #     table=BQ_TABLE,
        #     gcp_conn_id=GCP_CONN_ID,
        #     write_disposition=write_disposition,
        #     location="asia-southeast1",
        # )
        
        stats = load_dataframe_to_bq_upsert_with_counts(
            df=df,
            project_id=GCP_PROJECT,
            dataset=BQ_DATASET,
            target_table=BQ_TABLE,
            gcp_conn_id=GCP_CONN_ID,
            key_cols=["student_id"],                 # หรือ key ของคุณจริง ๆ
            location="US",
        )

        inserted = int(stats.get("inserted", 0))
        updated = int(stats.get("updated", 0))

        duration_sec = round(time.time() - start_ts, 2)

        # สำหรับ BQ truncate/append เรา map metric ให้เป็นมาตรฐานเดียวกับ invoice
        # - inserted: จำนวนแถวที่โหลดเข้า (มองเป็น inserted)
        # - updated: 0
        # - batches_total: 1 (โหลดครั้งเดียว)
        result = {
            "status": "success",
            "subject": "Education : Student Information",
            "inserted": inserted,
            "updated": updated,
            "duration_sec": duration_sec,
            "rows_total": rows_total,
            "batches_total": 1,
            "updated_samples": [],  # BQ load ไม่มี diff per row
            "target_table": f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
            "run_date": run_date,
        }

        # audit summary (ใช้ชุดเดียวกัน)
        write_audit_line({
            "ts": run_date,
            "pipeline": "student_information",
            "action": "SUMMARY",
            "write_disposition": write_disposition,
            **result
        })

        logger.info("✅ Loaded to BigQuery %s (%s rows) disposition=%s", result["target_table"], rows_total, write_disposition)
        return result
    
    @task(retries=0)
    def notify(load_result: dict):
        email_cfg = load_email_config_from_env()  # env-first แล้ว fallback conn (ตามที่คุณทำไว้)
        updated_samples = load_result.get("updated_samples") or []
        send_summary_email(load_result, email_cfg, updated_samples)

    start_ts = mark_start()
    raw_path = extract_to_file()
    clean_path = transform_file(raw_path)
    report = validate_file(clean_path)
    load_result = load(clean_path, report, start_ts)
    notify(load_result)


student_information_dag = student_information_etl()