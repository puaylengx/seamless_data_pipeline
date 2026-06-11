# dags/staging_student_dag.py
from __future__ import annotations

import time
from pathlib import Path
import pendulum
import pandas as pd
import logging
import math

from airflow.decorators import dag, task
from airflow.sdk import get_current_context
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook

# ------------------------------------------------------------------
# Project config / helpers
# ------------------------------------------------------------------
from src.config import DATA_DIR, get_conn, get_job_config_staging, get_staging_student_excel_path
from src.helpers.audit import write_audit_line
from src.helpers.emailer import load_email_config_from_env, send_summary_email

# ------------------------------------------------------------------
# Pipeline modules
# ------------------------------------------------------------------
from src.dgsi.staging_student.extractors.staging_student_sql_server import (
    build_engine_from_airflow_conn,
    extract_staging_student,
)
from src.dgsi.staging_student.transformers.staging_student import (
    transform_staging_student,
)
from src.dgsi.staging_student.loaders.staging_student_sql import (
    upload_temp_table,
    ensure_target_table,
    merge_all_in_batches,
    drop_temp_table,
)

logger = logging.getLogger("airflow.task")
TZ = "Asia/Bangkok"


# ======================================================================
# DAG
# ======================================================================
@dag(
    dag_id="staging_student_pipeline",
    start_date=pendulum.datetime(2026, 3, 17, tz=TZ),
    # schedule="@weekly",
    schedule="00 15 * * 5",  # นาที  ชั่วโมง  วันของเดือน  เดือน  วันของสัปดาห์
    catchup=False,
    default_args={"retries": 2},
    tags=["dgsi", "staging", "student", "staging-style"],
)
def staging_student_etl():

    # ------------------------------------------------------------
    # mark_start
    # ------------------------------------------------------------
    @task()
    def mark_start() -> float:
        return time.time()

    # ------------------------------------------------------------
    # extract (finance invoice style)
    # ------------------------------------------------------------
    @task()
    def extract_to_file() -> str:
        """
        ดึงข้อมูลจาก SQL Server → parquet
        ใช้ extractor แบบ finance invoice (รับ Airflow Connection object)
        """
        ctx = get_current_context()

        # ✅ ดึง Connection object จาก Airflow
        src_conn = BaseHook.get_connection("mssql_student_info")

        df = extract_staging_student(src_conn)

        run_id = ctx["run_id"].replace("|", "_").replace(":", "_")
        out_path = DATA_DIR / "extract" / f"staging_student_{run_id}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, index=False)

        logger.info("✅ Extracted rows=%s → %s", len(df), out_path)

        return str(out_path)

    # ------------------------------------------------------------
    # transform
    # ------------------------------------------------------------
    @task()
    def transform_file(raw_path: str) -> dict:
        """
        Data quality + normalize + merge Excel (talent/extra columns)
        """
        df = pd.read_parquet(raw_path)

        excel_path = get_staging_student_excel_path()

        df_clean, metrics = transform_staging_student(
            df,
            audit_writer=write_audit_line,
            excel_path=excel_path,
        )

        out_path = (
            Path(raw_path)
            .as_posix()
            .replace("/extract/", "/transform/")
            .replace("staging_student_", "staging_student_t_")
        )
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        df_clean.to_parquet(out_path, index=False)

        logger.info(
            "✅ Transform done rows=%s fixed=%s dropped=%s → %s",
            len(df_clean),
            metrics.get("fixed", 0),
            metrics.get("dropped", 0),
            out_path,
        )

        return {
            "path": str(out_path),
            "rows": len(df_clean),
            "metrics": metrics,
        }

    # ------------------------------------------------------------
    # load (SQL Server MERGE – finance invoice style)
    # ------------------------------------------------------------
    @task()
    def load(clean_path: str, report: dict, start_ts: float) -> dict:
        """
        Finance-invoice style load:
        - no_data branch
        - temp table upload
        - ensure target table
        - merge in batches + audit
        - drop temp
        - return standard payload for email
        """

        # -----------------------
        # no_data (เหมือน finance invoice)
        # -----------------------
        if report.get("rows", 0) == 0:
            return {
                "status": "no_data",
                "subject": "DGSI : Staging Student",  # ✅ เปลี่ยนชื่อได้ตาม pipeline
                "rows_total": 0,
                "inserted": 0,
                "updated": 0,
                "duration_sec": round(time.time() - start_ts, 2),
                "batches_total": 0,
                "updated_samples": [],
                "target_table": "-",
                "run_date": pendulum.now(TZ).format("YYYY-MM-DD HH:mm:ss"),
            }

        # -----------------------
        # config / connections
        # -----------------------
        job = (
            get_job_config_staging()
        )  # ต้องมี: target_schema, target_table, key_col, batch_size
        tgt = get_conn(
            "mssql_data_op"
        )  # หรือเปลี่ยนเป็น conn_id target ของ staging student

        # build target engine (เหมือน finance invoice)
        tgt_engine = build_engine_from_airflow_conn(tgt)

        temp_table = f"{job.target_table}_tmp"
        schema = job.target_schema

        # -----------------------
        # read data
        # -----------------------
        df = pd.read_parquet(clean_path)

        # (optional) กัน key missing
        if job.key_col not in df.columns:
            raise AirflowFailException(
                f"Missing key column '{job.key_col}' in dataframe"
            )

        # -----------------------
        # upload temp + ensure target
        # -----------------------
        upload_temp_table(df, tgt_engine, schema, temp_table, key_col=job.key_col)

        with tgt_engine.begin() as conn:
            ensure_target_table(
                conn,
                schema,
                job.target_table,
                df_columns=list(df.columns),
                key_col=job.key_col,
            )

        # -----------------------
        # merge (batch) + audit
        # -----------------------
        inserted, updated, updated_samples = merge_all_in_batches(
            tgt_engine=tgt_engine,
            schema=schema,
            target_table=job.target_table,
            key_col=job.key_col,
            batch_size=job.batch_size,
            audit_writer=write_audit_line,  # ✅ audit แบบ finance invoice
        )

        # drop temp
        with tgt_engine.begin() as conn:
            drop_temp_table(conn, schema, temp_table)

        # -----------------------
        # metrics
        # -----------------------
        duration_sec = round(time.time() - start_ts, 2)
        rows_total = int(report.get("rows", 0) or 0)
        batches_total = int(math.ceil(rows_total / job.batch_size)) if rows_total else 0

        # -----------------------
        # return (เหมือน finance invoice)
        # -----------------------
        return {
            "status": "success",
            "subject": "DGSI : Staging Student",  # ✅ ใช้กับ subject dynamic email
            "inserted": int(inserted),
            "updated": int(updated),
            "duration_sec": duration_sec,
            "rows_total": rows_total,
            "batches_total": batches_total,
            "updated_samples": (updated_samples or [])[:10],
            "target_table": f"{schema}.{job.target_table}",
            "run_date": pendulum.now(TZ).format("YYYY-MM-DD HH:mm:ss"),
        }

    # ------------------------------------------------------------
    # notify
    # ------------------------------------------------------------
    @task(retries=0)
    def notify(load_result: dict):
        email_cfg = load_email_config_from_env()
        #  ให้แน่ใจว่า load_result มี updated_samples (จาก loader) หรือส่งแยกก็ได้
        updated_samples = load_result.get("updated_samples") or []
        send_summary_email(load_result, email_cfg, updated_samples)

    # -------------------------------
    # DAG wiring
    # -------------------------------
    start_ts = mark_start()
    raw = extract_to_file()
    clean = transform_file(raw)
    result = load(clean["path"], clean, start_ts)
    notify(result)


# DAG object
staging_student_dag = staging_student_etl()
