from __future__ import annotations

import csv
import io
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping

from dotenv import load_dotenv
from google.cloud import storage

from extractors.student_information_sql_server import fetch_student_info
from helpers.logger import get_logger
from loaders.student_information_bigquery import (
    append_raw,
    bq_client,
    delete_table,
    fetch_change_log,
    load_staging,
    merge_to_curated
)
from transformers.student_information import normalize_student_df
from validators.student_information import ValidationError, validate_student_df
from config import PipelineConfig

# helper find project root run local
PROJECT_ROOT = Path(__file__).resolve().parents[3]
PACKAGE_ROOT = Path(__file__).resolve().parent

def _write_change_details_local(change_log: Iterable[Mapping[str, str]], date_str: str) -> None:
    change_dir = PROJECT_ROOT / "logs" / "education" / "student_information" / "change_details"
    change_dir.mkdir(parents=True, exist_ok=True)
    file_path = change_dir / f"student_information_changes_{date_str}.csv"
    write_header = not file_path.exists()
    
    timestamp = datetime.utcnow().isoformat()
    fieldnames = ["timestamp", "student_id", "field", "old_value", "new_value"]
    
    with file_path.open("a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for change in change_log:
            writer.writerow(
                {
                    "timestamp": timestamp,
                    "student_id": change.get("student_id"),
                    "field": change.get("field"),
                    "old_value": change.get("old_value"),
                    "new_value": change.get("new_value")
                }
            )

def _write_change_details_gcs(change_log: Iterable[Mapping[str, str]], date_str: str, bucket_name: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    path = f"education/student_information/change_details/student_information_changes_{date_str}.csv"
    
    blob = bucket.blob(path)
    buf = io.StringIO()
    fieldnames = ["timestamp", "student_id", "field", "old_value", "new_value"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    timestamp = datetime.utcnow().isoformat()
    
    for change in change_log:
        writer.writerow(
            {
                "timestamp": timestamp,
                "student_id": change.get("student_id"),
                "field": change.get("field"),
                "old_value": change.get("old_value"),
                "new_value": change.get("new_value")
            }
        )
    blob.upload_from_string(buf.getvalue(), content_type="test/csv")
    
def run_pipeline() -> None:
    date_str = datetime.utcnow().strftime("%Y%m%d")
    logger = get_logger(
        name=f"{__name__}/{date_str}",
        repo_root=PROJECT_ROOT,
        module_folder=Path("education") / "student_information",
        log_file_name=f"student_information_{date_str}.log",
    )
    
    # load env (local dev); on cloud run jobs reset password --set-env-vars/--set-secrets
    load_dotenv(PROJECT_ROOT / ".env", override=True)
    cfg = PipelineConfig.from_env(PACKAGE_ROOT)
    
    try:
        logger.info("Start extract...")
        df_raw = fetch_student_info(cfg)
        logger.info("Raw rows: %s", len(df_raw))
        
        logger.info("Normalizing...")
        df_norm = normalize_student_df(df_raw)
        
        logger.info("Loading to BigQuery (raw/staging/merge)...")
        with bq_client(cfg) as client:
            inserted_raw = append_raw(client, cfg, df_norm)
            stg_table = load_staging(client, cfg, df_norm)
            
            try:
                change_log = fetch_change_log(client, cfg, stg_table)
                merge_summary = merge_to_curated(client, cfg, stg_table)
                logger.info("Merge completed: job_id=%s", merge_summary["job_id"])
            finally:
                delete_table(client, stg_table)
        
        if change_log:
            for change in change_log:
                logger.info(
                    "Change detected | student=%s | field=%s | old=%s | new=%s",
                    change["student_id"],
                    change["field"],
                    change["old_value"],
                    change["new_value"]
                )
            bucket = os.getenv("AUDIT_GCS_BUCKET")
            if bucket:
                _write_change_details_gcs(change_log, date_str, bucket)
            else:
                _write_change_details_local(change_log, date_str)
        else:
            logger.info("No field-level changes detected.")
            
        logger.info("Done. inserted_raw=%s", inserted_raw)
        
    except ValidationError as error:
        logger.error("Validation field: %s", error)
        sys.exit(1)
    except Exception:
        logger.exception("ETL failed")
        sys.exit(1)

def main() -> None:
    run_pipeline()
    
if __name__ == "__main__":
    main()