# loaders/printer_solution_bigquery.py

from __future__ import annotations

import logging
from typing import Dict

import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

logger = logging.getLogger("airflow.task")

# -------------------------
# Helpers
# -------------------------
def _bq_table(project_id: str, dataset: str, table: str) -> str:
    return f"`{project_id}.{dataset}.{table}`"

def _quote_col(col: str) -> str:
    return f"`{col}`"

def _ensure_dataset_exists(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    location: str,
) -> None:
    ds_id = f"{project_id}.{dataset}"
    try:
        client.get_dataset(ds_id)
    except Exception:
        logger.info("Dataset not found, creating: %s (location=%s)", ds_id, location)
        ds = bigquery.Dataset(ds_id)
        ds.location = location
        client.create_dataset(ds, exists_ok=True)

def _ensure_table_exists(
    client: bigquery.Client,
    table_id_quoted: str,
    df: pd.DataFrame,
    location: str,
) -> None:
    """
    สร้างตาราง target ถ้ายังไม่มี โดยใช้ schema จาก df (autodetect)
    """
    table_id = table_id_quoted.replace("`", "")
    try:
        client.get_table(table_id)
        return
    except Exception:
        logger.info("Target table not found, creating: %s", table_id)

    job = client.load_table_from_dataframe(
        df.head(0),
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_EMPTY",
            autodetect=True,
        ),
        location=location,
    )
    job.result()

# -------------------------
# Public API: INSERT‑ONLY loader
# -------------------------
def load_printer_usage_monthly_insert_only(
    df: pd.DataFrame,
    *,
    project_id: str,
    dataset: str,
    target_table: str,
    gcp_conn_id: str,
    location: str = "asia-southeast1",
    staging_suffix: str = "_stg",
) -> Dict[str, int]:
    """
    ✅ INSERT‑ONLY (append) เข้า BigQuery
    ❌ ไม่ update / ไม่ merge ของเดิม

    Returns:
      {"inserted": int, "job_id": str}
    """
    if df is None or df.empty:
        logger.info("No data to load (empty dataframe).")
        return {"inserted": 0, "job_id": ""}

    logger.info(
        "INSERT‑ONLY load target table: %s.%s.%s",
        project_id, dataset, target_table
    )

    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, location=location)
    client: bigquery.Client = hook.get_client(
        project_id=project_id,
        location=location,
    )

    # ensure dataset + target
    _ensure_dataset_exists(client, project_id, dataset, location)

    staging_table = f"{target_table}{staging_suffix}"
    target_fqn = _bq_table(project_id, dataset, target_table)
    staging_fqn = _bq_table(project_id, dataset, staging_table)

    _ensure_table_exists(client, target_fqn, df, location)

    # 1) load to staging (truncate)
    staging_table_id = f"{project_id}.{dataset}.{staging_table}"
    load_job = client.load_table_from_dataframe(
        df,
        staging_table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        ),
        location=location,
    )
    load_job.result()
    job_id = getattr(load_job, "job_id", "")

    # 2) INSERT INTO target SELECT * FROM staging
    cols = ", ".join([_quote_col(c) for c in df.columns])

    insert_sql = f"""
        INSERT INTO {target_fqn} ({cols})
        SELECT {cols}
        FROM {staging_fqn}
    """

    client.query(insert_sql, location=location).result()

    inserted_count = len(df)

    logger.info(
        "BQ insert‑only done: inserted=%s target=%s job_id=%s",
        inserted_count, target_fqn, job_id
    )

    return {
        "inserted": inserted_count,
        "job_id": job_id,
    }