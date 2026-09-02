from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence, Union

import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

logger = logging.getLogger("airflow.task")

# คีย์ระดับ "ยอดรวมรายเดือนของผู้ใช้" — ไม่รวม full_name/department/office
# เพราะค่าพวกนั้นเป็น attribute ที่เปลี่ยนได้ (ย้ายแผนก/เปลี่ยนชื่อ)
# ถ้าเอามาเป็นคีย์จะกลายเป็นแถวใหม่และนับยอดซ้ำ
DEFAULT_KEY_COLS: List[str] = [
    "user_name",
    "job_type",
    "usage_calendar_year",
    "usage_calendar_month",
]

# คอลัมน์ที่เปลี่ยนทุกรอบอยู่แล้ว ไม่ควรนับว่าเป็น "การเปลี่ยนแปลงของข้อมูล"
DEFAULT_COMPARE_EXCLUDE: List[str] = ["ingestion_date"]

# เดือนที่จะถูกเขียนทับ (ลบทิ้งก่อน insert ใหม่)
DEFAULT_PARTITION_COLS: List[str] = ["usage_calendar_year", "usage_calendar_month"]


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
) -> bool:
    """
    สร้างตาราง target ถ้ายังไม่มี โดยใช้ schema จาก df
    return True ถ้าเพิ่งสร้างใหม่ (ตารางว่าง)
    """
    table_id = table_id_quoted.replace("`", "")
    try:
        client.get_table(table_id)
        return False
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
    return True


def _join_condition(keys: Sequence[str], t_alias: str = "T", s_alias: str = "S") -> str:
    """
    เงื่อนไข join ที่เทียบ NULL = NULL ได้:
      (T.k = S.k OR (T.k IS NULL AND S.k IS NULL)) AND ...
    """
    parts = []
    for k in keys:
        qc = _quote_col(k)
        parts.append(
            f"(({t_alias}.{qc} = {s_alias}.{qc}) "
            f"OR ({t_alias}.{qc} IS NULL AND {s_alias}.{qc} IS NULL))"
        )
    return " AND ".join(parts)


def _struct_json(alias: str, cols: Sequence[str]) -> str:
    """TO_JSON_STRING(STRUCT(...)) สำหรับเทียบว่าค่าเปลี่ยนไหม (รองรับ NULL อัตโนมัติ)"""
    fields = ", ".join(f"{alias}.{_quote_col(c)} AS {_quote_col(c)}" for c in cols)
    return f"TO_JSON_STRING(STRUCT({fields}))"


def _dedup_cte(fqn: str, keys: Sequence[str], order_cols: Sequence[str]) -> str:
    """
    เลือก 1 แถวต่อ key

    - target: จำเป็น เพราะอาจมีแถวซ้ำค้างจาก loader เวอร์ชันเดิม
      (เรียงด้วย ingestion_date DESC เพื่อเก็บแถวล่าสุด)
    - staging: ปกติ query ต้นทาง GROUP BY มาแล้ว แต่ถ้า tbl_user มี user_name
      ซ้ำกันคนละ user_id ก็จะซ้ำได้ จึงต้องเรียงแบบชี้ขาด ไม่ใช่ ORDER BY 1
    """
    part_sql = ", ".join(_quote_col(k) for k in keys)
    order_sql = ", ".join(order_cols)
    return f"""
        SELECT * EXCEPT(__rn) FROM (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY {part_sql} ORDER BY {order_sql}
          ) AS __rn
          FROM {fqn}
        )
        WHERE __rn = 1
    """


def _count_changes(
    client: bigquery.Client,
    *,
    staging_fqn: str,
    target_fqn: str,
    keys: Sequence[str],
    compare_cols: Sequence[str],
    staging_order: Sequence[str],
    target_order: Sequence[str],
    location: str,
) -> Dict[str, int]:
    """
    นับ insert / update / unchanged ก่อนเขียนจริง
    (BigQuery ไม่แยกตัวเลขนี้ให้จาก MERGE หรือ DELETE+INSERT)
    """
    sql = f"""
        WITH S AS ({_dedup_cte(staging_fqn, keys, staging_order)}),
             T AS (
               SELECT *, TRUE AS __matched
               FROM ({_dedup_cte(target_fqn, keys, target_order)})
             ),
             J AS (
               SELECT
                 IFNULL(T.__matched, FALSE) AS matched,
                 {_struct_json('S', compare_cols)} AS s_json,
                 {_struct_json('T', compare_cols)} AS t_json
               FROM S
               LEFT JOIN T ON {_join_condition(keys)}
             )
        SELECT
          COUNTIF(NOT matched)                        AS to_insert,
          COUNTIF(matched AND s_json != t_json)       AS to_update,
          COUNTIF(matched AND s_json  = t_json)       AS unchanged
        FROM J
    """
    row = next(iter(client.query(sql, location=location).result()))
    return {
        "to_insert": int(row["to_insert"] or 0),
        "to_update": int(row["to_update"] or 0),
        "unchanged": int(row["unchanged"] or 0),
    }


def _fetch_change_samples(
    client: bigquery.Client,
    *,
    staging_fqn: str,
    target_fqn: str,
    keys: Sequence[str],
    compare_cols: Sequence[str],
    staging_order: Sequence[str],
    target_order: Sequence[str],
    location: str,
    limit: int,
) -> List[Dict[str, Any]]:
    """ดึงตัวอย่างแถวที่ค่าเปลี่ยน พร้อมค่าเก่า/ค่าใหม่ ไว้แสดงในอีเมลสรุป"""
    if limit <= 0:
        return []

    key_sql = ", ".join(f"S.{_quote_col(k)} AS {_quote_col(k)}" for k in keys)
    pair_sql = ", ".join(
        f"S.{_quote_col(c)} AS {_quote_col('new__' + c)}, "
        f"T.{_quote_col(c)} AS {_quote_col('old__' + c)}"
        for c in compare_cols
    )

    sql = f"""
        WITH S AS ({_dedup_cte(staging_fqn, keys, staging_order)}),
             T AS (
               SELECT *, TRUE AS __matched
               FROM ({_dedup_cte(target_fqn, keys, target_order)})
             )
        SELECT {key_sql}, {pair_sql}
        FROM S
        JOIN T ON {_join_condition(keys)}
        WHERE {_struct_json('S', compare_cols)} != {_struct_json('T', compare_cols)}
        ORDER BY {', '.join(f'S.{_quote_col(k)}' for k in keys)}
        LIMIT {int(limit)}
    """

    samples: List[Dict[str, Any]] = []
    for row in client.query(sql, location=location).result():
        changes = {}
        for c in compare_cols:
            old_v, new_v = row[f"old__{c}"], row[f"new__{c}"]
            if old_v != new_v:
                changes[c] = {"old": old_v, "new": new_v}
        if not changes:
            continue
        sample: Dict[str, Any] = {k: row[k] for k in keys}
        # emailer ใช้ key "code" เป็นตัวระบุแถว
        sample["code"] = row[keys[0]]
        sample["changes"] = changes
        samples.append(sample)
    return samples


# -------------------------
# Public API: upsert รายเดือน (delete + insert ใน transaction)
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
    unique_key_cols: Optional[List[str]] = None,
    partition_cols: Optional[List[str]] = None,
    compare_exclude_cols: Optional[List[str]] = None,
    sample_limit: int = 20,
) -> Dict[str, Union[int, str, list]]:
    """
    เขียนยอดพิมพ์รายเดือนเข้า BigQuery แบบ idempotent

    ทำไมไม่ใช้ insert-only:
      query ต้นทาง GROUP BY ระดับเดือน ยอดจึงโตขึ้นทุกวันระหว่างเดือน
      ถ้า insert เฉพาะ key ที่ยังไม่มี ยอดของเดือนจะค้างอยู่ที่รอบแรกตลอดไป

    วิธีเขียน: ลบทุกแถวของ "เดือนที่มีใน staging" แล้ว insert ชุดใหม่
    ทั้งหมดอยู่ใน transaction เดียว และล้างแถวซ้ำที่ค้างจาก loader เดิมไปในตัว

    Returns:
      {"inserted", "updated", "unchanged", "job_id", "updated_samples"}
    """
    empty: Dict[str, Union[int, str, list]] = {
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
        "job_id": "",
        "updated_samples": [],
    }
    if df is None or df.empty:
        logger.info("No data to load (empty dataframe).")
        return empty

    cols = list(df.columns)
    keys = unique_key_cols or [k for k in DEFAULT_KEY_COLS if k in cols]
    parts = partition_cols or [k for k in DEFAULT_PARTITION_COLS if k in cols]
    excluded = set(compare_exclude_cols or DEFAULT_COMPARE_EXCLUDE) | set(keys)
    compare_cols = [c for c in cols if c not in excluded]
    # ลำดับตัดสินเมื่อเจอ key ซ้ำ: target เอาแถวล่าสุด, staging เรียงทุกคอลัมน์ให้ผลคงที่
    target_order = ["`ingestion_date` DESC"] if "ingestion_date" in cols else [
        _quote_col(c) for c in compare_cols
    ]
    staging_order = [_quote_col(c) for c in compare_cols]

    for name, required in (("unique_key_cols", keys), ("partition_cols", parts)):
        missing = [c for c in required if c not in cols]
        if missing:
            raise ValueError(f"{name} not found in dataframe: {missing}")
    if not compare_cols:
        raise ValueError("No columns left to compare; check compare_exclude_cols/keys")

    logger.info(
        "Upsert monthly printer usage → %s.%s.%s (keys=%s, months rewritten by %s)",
        project_id, dataset, target_table, keys, parts,
    )

    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, location=location)
    client: bigquery.Client = hook.get_client(project_id=project_id, location=location)

    _ensure_dataset_exists(client, project_id, dataset, location)

    staging_table = f"{target_table}{staging_suffix}"
    target_fqn = _bq_table(project_id, dataset, target_table)
    staging_fqn = _bq_table(project_id, dataset, staging_table)

    is_new_table = _ensure_table_exists(client, target_fqn, df, location)

    # 1) โหลดลง staging (truncate ทุกรอบ)
    load_job = client.load_table_from_dataframe(
        df,
        f"{project_id}.{dataset}.{staging_table}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        location=location,
    )
    load_job.result()
    job_id = getattr(load_job, "job_id", "")

    # 2) นับ insert/update/unchanged ก่อนเขียน (ต้องนับก่อน เพราะขั้นที่ 3 ลบของเดิมทิ้ง)
    if is_new_table:
        counts = {"to_insert": len(df), "to_update": 0, "unchanged": 0}
        samples: List[Dict[str, Any]] = []
    else:
        counts = _count_changes(
            client,
            staging_fqn=staging_fqn,
            target_fqn=target_fqn,
            keys=keys,
            compare_cols=compare_cols,
            staging_order=staging_order,
            target_order=target_order,
            location=location,
        )
        samples = _fetch_change_samples(
            client,
            staging_fqn=staging_fqn,
            target_fqn=target_fqn,
            keys=keys,
            compare_cols=compare_cols,
            staging_order=staging_order,
            target_order=target_order,
            location=location,
            limit=sample_limit,
        )

    # 3) เขียนจริง: ลบเดือนที่กระทบ แล้ว insert ชุดใหม่ ภายใน transaction เดียว
    cols_sql = ", ".join(_quote_col(c) for c in cols)
    exists_sql = " AND ".join(
        f"(S.{_quote_col(c)} = T.{_quote_col(c)} "
        f"OR (S.{_quote_col(c)} IS NULL AND T.{_quote_col(c)} IS NULL))"
        for c in parts
    )
    write_sql = f"""
        BEGIN TRANSACTION;

        DELETE FROM {target_fqn} AS T
        WHERE EXISTS (
          SELECT 1 FROM {staging_fqn} AS S WHERE {exists_sql}
        );

        INSERT INTO {target_fqn} ({cols_sql})
        SELECT {cols_sql}
        FROM ({_dedup_cte(staging_fqn, keys, staging_order)});

        COMMIT TRANSACTION;
    """
    write_job = client.query(write_sql, location=location)
    write_job.result()

    result: Dict[str, Union[int, str, list]] = {
        "inserted": counts["to_insert"],
        "updated": counts["to_update"],
        "unchanged": counts["unchanged"],
        "job_id": job_id,
        "updated_samples": samples,
    }

    logger.info(
        "BQ upsert done: inserted=%s updated=%s unchanged=%s target=%s load_job_id=%s",
        result["inserted"], result["updated"], result["unchanged"], target_fqn, job_id,
    )
    return result
