from __future__ import annotations

import logging
from typing import Dict, List, Optional, Union

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

def _merge_join_condition(keys: List[str], t_alias: str = "T", s_alias: str = "S") -> str:
    """
    สร้างเงื่อนไข join ที่เทียบ NULL = NULL ได้:
      (T.k = S.k OR (T.k IS NULL AND S.k IS NULL)) AND ...
    """
    parts = []
    for k in keys:
        qc = _quote_col(k)
        parts.append(f"(({t_alias}.{qc} = {s_alias}.{qc}) OR ({t_alias}.{qc} IS NULL AND {s_alias}.{qc} IS NULL))")
    return " AND ".join(parts)

def _staging_dedup_select(staging_fqn: str, cols: List[str], keys: List[str]) -> str:
    """
    กัน duplicate ภายใน staging เอง (ถ้ามี) โดยเลือก 1 แถวต่อ key
    (ไม่มี timestamp ให้ order ก็ใช้ ORDER BY 1 เฉย ๆ เพื่อให้ SQL ถูกต้อง)
    """
    cols_sql = ", ".join([f"S.{_quote_col(c)}" for c in cols])
    part_sql = ", ".join([_quote_col(k) for k in keys])
    return f"""
        SELECT {cols_sql}
        FROM {staging_fqn} AS S
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {part_sql} ORDER BY 1) = 1
    """

# -------------------------
# Public API: INSERT‑ONLY incremental loader
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
    unique_key_cols: Optional[List[str]] = None,
) -> Dict[str, Union[int, str]]:
    """
    ✅ INSERT‑ONLY เข้า BigQuery แต่ insert เฉพาะ "ของใหม่"
    ❌ ไม่ update / ไม่ merge ของเดิม (เฉพาะ WHEN NOT MATCHED)

    unique_key_cols:
      - ระบุคอลัมน์ที่ใช้เป็น "ความเป็นเอกลักษณ์" ของแถว
      - ถ้าไม่ส่งมา จะ fallback เป็น df.columns (คือกันซ้ำแบบทั้งแถวเหมือนกัน 100%)
        *แนะนำให้ระบุ key จริง ๆ เพื่อให้เร็วและชัวร์*

    Returns:
      {"inserted": int, "job_id": str}
    """
    if df is None or df.empty:
        logger.info("No data to load (empty dataframe).")
        return {"inserted": 0, "job_id": ""}

    # เลือก key: ถ้าไม่ระบุ -> ใช้ทุกคอลัมน์ (กันซ้ำแบบ exact row)
    keys = unique_key_cols or list(df.columns)

    # sanity: key ต้องอยู่ใน df
    missing_keys = [k for k in keys if k not in df.columns]
    if missing_keys:
        raise ValueError(f"unique_key_cols not found in dataframe: {missing_keys}")

    logger.info(
        "Incremental INSERT‑ONLY load target table: %s.%s.%s (keys=%s)",
        project_id, dataset, target_table, keys
    )

    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, location=location)
    client: bigquery.Client = hook.get_client(project_id=project_id, location=location)

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

    # 2) MERGE: insert only when not matched (by keys)
    cols = list(df.columns)
    cols_sql = ", ".join([_quote_col(c) for c in cols])
    values_sql = ", ".join([f"S.{_quote_col(c)}" for c in cols])

    join_sql = _merge_join_condition(keys, t_alias="T", s_alias="S")
    source_sql = _staging_dedup_select(staging_fqn, cols, keys)

    merge_sql = f"""
        MERGE {target_fqn} AS T
        USING ({source_sql}) AS S
        ON {join_sql}
        WHEN NOT MATCHED THEN
          INSERT ({cols_sql}) VALUES ({values_sql})
    """

    merge_job = client.query(merge_sql, location=location)
    merge_job.result()

    inserted = int(getattr(merge_job, "num_dml_affected_rows", 0) or 0)

    logger.info(
        "BQ incremental insert‑only done: inserted=%s target=%s load_job_id=%s",
        inserted, target_fqn, job_id
    )

    return {
        "inserted": inserted,
        "job_id": job_id,
    }