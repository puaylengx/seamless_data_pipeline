from airflow.decorators import dag, task
import pendulum
from airflow.exceptions import AirflowFailException
import os
import pandas as pd
import time
import math

from src.config import get_conn, get_job_config, PathConfig
from src.dgsi.finance_invoice.extractors.finance_invoice_sql_server import extract_invoices
from src.dgsi.finance_invoice.transformers.finance_invoice import transform
from src.dgsi.finance_invoice.validators.finance_invoice import validate

# loader/emailer คุณ import ตามที่คุณจัด
from src.dgsi.finance_invoice.extractors.finance_invoice_sql_server import build_engine_from_airflow_conn
from src.dgsi.finance_invoice.loaders.finance_invoice_sql import (
    upload_temp_table, ensure_target_table, merge_all_in_batches, drop_temp_table
)

from src.helpers.audit import write_audit_line

# for send email
from src.helpers.emailer import load_email_config_from_env, send_summary_email

TZ = "Asia/Bangkok"

@dag(
    dag_id="finance_invoice_pipeline",
    start_date=pendulum.datetime(2026, 3, 17, tz=TZ),
    # schedule="@weekly",
    schedule="00 15 * * 5", # นาที  ชั่วโมง  วันของเดือน  เดือน  วันของสัปดาห์
    catchup=False,
    tags=["finance", "etl"],
    default_args={"retries": 2},
)

def finance_invoice_etl():
    
    @task()
    def mark_start() -> float:
        return time.time()

    @task()
    def extract_to_file() -> str:
        src = get_conn("mssql_student_info")      # Airflow Connection ID
        paths = PathConfig()
        os.makedirs(paths.workdir, exist_ok=True)
        out = os.path.join(paths.workdir, "raw.parquet")

        df = extract_invoices(src)
        if df.empty:
            # ให้ downstream รู้ว่าไม่มีข้อมูล
            df.to_parquet(out, index=False)
            return out

        df.to_parquet(out, index=False)
        return out

    @task()
    def transform_file(raw_path: str) -> str:
        paths = PathConfig()
        out = os.path.join(paths.workdir, "clean.parquet")

        df = pd.read_parquet(raw_path)
        if df.empty:
            df.to_parquet(out, index=False)
            return out

        clean = transform(df)
        clean.to_parquet(out, index=False)
        return out

    @task()
    def validate_file(clean_path: str) -> dict:
        df = pd.read_parquet(clean_path)
        report = validate(df)
        if not report["ok"]:
            raise AirflowFailException(f"Validation failed: {report}")
        return report

    @task()
    def load(clean_path: str, report: dict, start_ts: float) -> dict:
        if report.get("rows", 0) == 0:
            return {
                "status": "no_data",
                "subject": "DGSI : Finance Invoice",
                "rows_total": 0,
                "inserted": 0,
                "updated": 0,
                "duration_sec": round(time.time() - start_ts, 2),
                "batches_total": 0,
                "updated_samples": [],
                "target_table": "-",
                "run_date": pendulum.now(TZ).format("YYYY-MM-DD HH:mm:ss"),
            }

        job = get_job_config()
        tgt = get_conn("mssql_data_op")

        # build target engine
        tgt_engine = build_engine_from_airflow_conn(tgt)

        temp_table = f"{job.target_table}_tmp"
        schema = job.target_schema

        df = pd.read_parquet(clean_path)

        upload_temp_table(df, tgt_engine, schema, temp_table)

        with tgt_engine.begin() as conn:
            ensure_target_table(conn, schema, job.target_table)

        # merge
        inserted, updated, updated_samples = merge_all_in_batches(
            tgt_engine=tgt_engine,
            schema=schema,
            target_table=job.target_table,
            key_col=job.key_col,
            batch_size=job.batch_size,
            audit_writer=write_audit_line,  # ผูกกับ audit.py ได้
        )

        with tgt_engine.begin() as conn:
            drop_temp_table(conn, schema, temp_table)
            
        # ✅ duration ใช้ start_ts จาก task runtime จริง
        duration_sec = round(time.time() - start_ts, 2)

        rows_total = int(report.get("rows", 0) or 0)
        batches_total = int(math.ceil(rows_total / job.batch_size)) if rows_total else 0

        return {
            "status": "success",
            "subject": "DGSI : Finance Invoice",
            "inserted": int(inserted),
            "updated": int(updated),
            "duration_sec": duration_sec,
            "rows_total": rows_total,
            "batches_total":batches_total,
            "updated_samples": (updated_samples or [])[:10],
            "target_table": f"{schema}.{job.target_table}",
            "run_date": pendulum.now(TZ).format("YYYY-MM-DD HH:mm:ss"),
        }


    @task(retries=0)  # ในฟังก์ชันมี retry 3 ครั้งแล้ว
    def notify(load_result: dict):
        email_cfg = load_email_config_from_env()
        # ให้แน่ใจว่า load_result มี updated_samples (จาก loader) หรือส่งแยกก็ได้
        updated_samples = load_result.get("updated_samples") or []
        send_summary_email(load_result, email_cfg, updated_samples)
    
    # @task()
    # def should_send_email(result: dict) -> bool:
    #     return (result.get("inserted", 0) + result.get("updated", 0)) > 0
    
    start_ts = mark_start()
    raw = extract_to_file()
    clean = transform_file(raw)
    rep = validate_file(clean)
    load_result = load(clean, rep, start_ts)
    
    notify(load_result)

finance_invoice_etl()