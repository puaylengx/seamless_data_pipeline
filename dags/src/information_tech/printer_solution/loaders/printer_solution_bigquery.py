# loaders/printer_solution_bigquery.py

from __future__ import annotations

import logging
from typing import List, Optional, Dict

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

def _hash_expr(alias: str, cols: List[str]) -> str:
    """
    รวมหลายคอลัมน์เป็น hash เพื่อเช็คว่า row เปลี่ยนจริงไหม (ลด update ที่ไม่จำเป็น)
    """
    struct_cols = ", ".join([f"{alias}.{_quote_col(c)} AS {_quote_col(c)}" for c in cols])
    return f"FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({struct_cols})))"

def _ensure_dataset_exists(client: bigquery.Client, project_id: str, dataset: str, location: str) -> None:
    ds_id = f"{project_id}.{dataset}"
    try:
        client.get_dataset(ds_id)
    except Exception:
        logger.info("Dataset not found, creating: %s (location=%s)", ds_id, location)
        ds = bigquery.Dataset(ds_id)
        ds.location = location  # ✅ กัน dataset ไปโผล่ US โดยไม่ตั้งใจ
        client.create_dataset(ds, exists_ok=True)

def _ensure_table_exists(client: bigquery.Client, table_id_quoted: str, df: pd.DataFrame, location: str) -> None:
    """
    สร้างตาราง target ถ้ายังไม่มี โดยใช้ schema จาก df.head(0) และ autodetect
    """
    table_id = table_id_quoted.replace("`", "")
    try:
        client.get_table(table_id)
        return
    except Exception:
        logger.info("Target table not found, creating: %s", table_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_EMPTY",
        autodetect=True,
    )
    job = client.load_table_from_dataframe(
        df.head(0),
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()

def _build_deduped_source_subquery(
    staging_fqn: str,
    key_cols: List[str],
    order_by_expr: str,
) -> str:
    """
    สร้าง subquery สำหรับใช้ใน MERGE ... USING (...) เพื่อ deduplicate 1 row ต่อ key
    หมายเหตุ: เพื่อเลี่ยง Syntax error "Unexpected keyword MERGE" เราจะไม่ใช้ WITH นำหน้า MERGE
    """
    key_partition = ", ".join([_quote_col(k) for k in key_cols])

    # ✅ subquery (deduped source)
    return f"""
    (
      SELECT * EXCEPT(_rn)
      FROM (
        SELECT
          S.*,
          ROW_NUMBER() OVER (
            PARTITION BY {key_partition}
            ORDER BY {order_by_expr} DESC
          ) AS _rn
        FROM {staging_fqn} S
      )
      WHERE _rn = 1
    )
    """

# -------------------------
# Public API: specific loader for printer usage monthly
# -------------------------
def load_printer_usage_monthly_upsert(
    df: pd.DataFrame,
    *,
    project_id: str,
    dataset: str,
    target_table: str,
    gcp_conn_id: str,
    location: str = "asia-southeast1",
    staging_suffix: str = "_stg",
    key_cols: Optional[List[str]] = None,
    exclude_update_cols: Optional[List[str]] = None,
) -> Dict[str, int]:
    """
    ✅ Upsert (MERGE) เข้า BigQuery + นับ inserted/updated
    เหมาะกับตาราง aggregated monthly usage

    Returns:
      {"inserted": int, "updated": int, "job_id": str}
    """
    if df is None or df.empty:
        logger.info("No data to load (empty dataframe).")
        return {"inserted": 0, "updated": 0, "job_id": ""}

    # กัน update พุ่งทุกครั้ง (ถ้ามีคอลัมน์ ingestion_date / loaded_at ฯลฯ)
    if exclude_update_cols is None:
        exclude_update_cols = ["ingestion_date", "loaded_at"]

    logger.info("MERGE target table: %s.%s.%s", project_id, dataset, target_table)

    # ✅ key ของ monthly aggregated row ตามที่คุณระบุ
    if not key_cols:
        key_cols = [
            "user_name",
            "usage_budget_year",
            "usage_calendar_month",
        ]

    # ตรวจว่า key อยู่ใน df จริง
    missing_keys = [k for k in key_cols if k not in df.columns]
    if missing_keys:
        raise ValueError(f"Missing key columns in df: {missing_keys}")

    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, location=location)
    client: bigquery.Client = hook.get_client(project_id=project_id, location=location)

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

    # 2) Prepare columns
    exclude_update_cols = [c for c in exclude_update_cols if c in df.columns]
    non_key_cols = [c for c in df.columns if c not in key_cols and c not in exclude_update_cols]

    # ใช้ hash เป็น deterministic order ตอน dedup ถ้าไม่มีคอลัมน์เวลา (updated_at) ให้ใช้วิธีนี้แทน
    dedup_hash_cols = non_key_cols if non_key_cols else key_cols
    order_by_expr = _hash_expr("S", dedup_hash_cols)

    # ✅ สร้าง deduped source subquery (ใช้ทั้งนับและ merge ให้ตรงกัน)
    src_using = _build_deduped_source_subquery(
        staging_fqn=staging_fqn,
        key_cols=key_cols,
        order_by_expr=order_by_expr,
    )

    # join condition
    join_cond = " AND ".join([f"T.{_quote_col(k)} = SRC.{_quote_col(k)}" for k in key_cols])

    # 2a) Counts (insert/update) — ใช้ SRC ที่ dedup แล้ว
    inserted_sql = f"""
    SELECT COUNT(1) AS inserted_count
    FROM {src_using} SRC
    LEFT JOIN {target_fqn} T
      ON {join_cond}
    WHERE {" OR ".join([f"T.{_quote_col(k)} IS NULL" for k in key_cols])}
    """

    if non_key_cols:
        updated_sql = f"""
        SELECT COUNT(1) AS updated_count
        FROM {src_using} SRC
        JOIN {target_fqn} T
          ON {join_cond}
        WHERE {_hash_expr("SRC", non_key_cols)} != {_hash_expr("T", non_key_cols)}
        """
    else:
        updated_sql = "SELECT 0 AS updated_count"

    ins_row = list(client.query(inserted_sql, location=location).result())[0]
    upd_row = list(client.query(updated_sql, location=location).result())[0]
    inserted_count = int(ins_row["inserted_count"])
    updated_count = int(upd_row["updated_count"])

    # 3) MERGE
    insert_cols = ", ".join([_quote_col(c) for c in df.columns])
    insert_vals = ", ".join([f"SRC.{_quote_col(c)}" for c in df.columns])

    # ✅ อัปเดตเฉพาะคอลัมน์ที่ไม่ใช่ key และไม่อยู่ใน exclude_update_cols
    update_cols = [c for c in df.columns if c not in key_cols and c not in exclude_update_cols]

    if update_cols:
        set_clause = ",\n                ".join([f"{_quote_col(c)} = SRC.{_quote_col(c)}" for c in update_cols])

        if non_key_cols:
            merge_sql = f"""
            MERGE {target_fqn} T
            USING {src_using} SRC
            ON {join_cond}
            WHEN MATCHED AND ({_hash_expr("SRC", non_key_cols)} != {_hash_expr("T", non_key_cols)}) THEN
              UPDATE SET
                {set_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols})
              VALUES ({insert_vals})
            """
        else:
            merge_sql = f"""
            MERGE {target_fqn} T
            USING {src_using} SRC
            ON {join_cond}
            WHEN MATCHED THEN
              UPDATE SET
                {set_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols})
              VALUES ({insert_vals})
            """
    else:
        merge_sql = f"""
        MERGE {target_fqn} T
        USING {src_using} SRC
        ON {join_cond}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_vals})
        """

    client.query(merge_sql, location=location).result()

    logger.info(
        "BQ upsert done: inserted=%s updated=%s target=%s job_id=%s",
        inserted_count, updated_count, target_fqn, job_id
    )
    return {"inserted": inserted_count, "updated": updated_count, "job_id": job_id}