from __future__ import annotations

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
import pandas as pd
from typing import Iterable, List, Tuple, Optional
import logging

logger = logging.getLogger("airflow.task")


def _bq_table(project_id: str, dataset: str, table: str) -> str:
    return f"`{project_id}.{dataset}.{table}`"


def _quote_col(col: str) -> str:
    return f"`{col}`"


def _hash_expr(alias: str, cols: List[str]) -> str:
    """
    รวมหลายคอลัมน์เป็น hash เพื่อเช็คว่า row เปลี่ยนจริงไหม
    """
    struct_cols = ", ".join([f"{alias}.{_quote_col(c)} AS {_quote_col(c)}" for c in cols])
    return f"FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({struct_cols})))"


def _ensure_table_exists(client: bigquery.Client, table_id: str, df: pd.DataFrame, location: str):
    """
    สร้างตาราง target ถ้ายังไม่มี โดยใช้ schema จาก df.head(0)
    """
    try:
        client.get_table(table_id.replace("`", ""))
        return
    except Exception:
        logger.info("Target table not found, creating: %s", table_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_EMPTY",
        autodetect=True,
    )
    job = client.load_table_from_dataframe(
        df.head(0),
        table_id.replace("`", ""),
        job_config=job_config,
        location=location,
    )
    job.result()


def load_dataframe_to_bq_upsert_with_counts(
    df: pd.DataFrame,
    project_id: str,
    dataset: str,
    target_table: str,
    gcp_conn_id: str,
    key_cols: Optional[List[str]] = None,
    location: str = "asia-southeast1",
    staging_suffix: str = "_stg",
    exclude_update_cols: Optional[List[str]] = None,
) -> dict:
    """
    ✅ Upsert เข้า BQ + นับ inserted/updated

    Returns: {"inserted": int, "updated": int}
    """
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, location=location)
    client: bigquery.Client = hook.get_client(project_id=project_id, location=location)  # [1](https://www.iditect.com/faq/python/how-to-set-up-airflow-send-email.html)

    if df is None or df.empty:
        return {"inserted": 0, "updated": 0}

    if exclude_update_cols is None:
        exclude_update_cols = ["ingestion_date"]  # ✅ กัน updated พุ่งทุกครั้ง

    # default key
    if not key_cols:
        if "student_id" in df.columns:
            key_cols = ["student_id"]
        elif "code" in df.columns:
            key_cols = ["code"]
        else:
            raise ValueError("key_cols is required because df has no 'student_id' or 'code'")

    staging_table = f"{target_table}{staging_suffix}"

    target_fqn = _bq_table(project_id, dataset, target_table)
    staging_fqn = _bq_table(project_id, dataset, staging_table)

    # ensure target exists
    _ensure_table_exists(client, target_fqn, df, location)

    # 1) load to staging (truncate always)
    staging_table_id = f"{project_id}.{dataset}.{staging_table}"
    load_job = client.load_table_from_dataframe(
        df,
        staging_table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True),
        location=location,
    )
    load_job.result()

    # 2) counts (insert/update)
    non_key_cols = [c for c in df.columns if c not in key_cols and c not in exclude_update_cols]
    join_cond = " AND ".join([f"T.{_quote_col(k)} = S.{_quote_col(k)}" for k in key_cols])

    inserted_sql = f"""
    SELECT COUNT(1) AS inserted_count
    FROM {staging_fqn} S
    LEFT JOIN {target_fqn} T
      ON {join_cond}
    WHERE {" OR ".join([f"T.{_quote_col(k)} IS NULL" for k in key_cols])}
    """

    if non_key_cols:
        s_hash = _hash_expr("S", non_key_cols)
        t_hash = _hash_expr("T", non_key_cols)
        updated_sql = f"""
        SELECT COUNT(1) AS updated_count
        FROM {staging_fqn} S
        JOIN {target_fqn} T
          ON {join_cond}
        WHERE {s_hash} != {t_hash}
        """
    else:
        updated_sql = "SELECT 0 AS updated_count"

    ins_row = list(client.query(inserted_sql, location=location).result())[0]
    upd_row = list(client.query(updated_sql, location=location).result())[0]
    inserted_count = int(ins_row["inserted_count"])
    updated_count = int(upd_row["updated_count"])

    # 3) MERGE
    insert_cols = ", ".join([_quote_col(c) for c in df.columns])
    insert_vals = ", ".join([f"S.{_quote_col(c)}" for c in df.columns])

    if non_key_cols:
        set_clause = ",\n    ".join([f"{_quote_col(c)} = S.{_quote_col(c)}" for c in non_key_cols + exclude_update_cols if c in df.columns and c not in key_cols])
        # เงื่อนไข update: เปลี่ยนจริงเท่านั้น
        merge_sql = f"""
        MERGE {target_fqn} T
        USING {staging_fqn} S
        ON {join_cond}
        WHEN MATCHED AND ({_hash_expr("S", non_key_cols)} != {_hash_expr("T", non_key_cols)}) THEN
          UPDATE SET
            {set_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_vals})
        """
    else:
        merge_sql = f"""
        MERGE {target_fqn} T
        USING {staging_fqn} S
        ON {join_cond}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_vals})
        """

    client.query(merge_sql, location=location).result()  # [2](https://stackoverflow.com/questions/55626195/export-all-airflow-connections-to-new-environment)

    logger.info("BQ upsert: inserted=%s updated=%s target=%s", inserted_count, updated_count, target_fqn)
    return {"inserted": inserted_count, "updated": updated_count}
