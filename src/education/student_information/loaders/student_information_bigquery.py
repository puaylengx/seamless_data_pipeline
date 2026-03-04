from __future__ import annotations
from contextlib import contextmanager
from typing import Iterable, Mapping
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, LoadJobConfig, TimePartitioning, TimePartitioningType
import pandas as pd
from datetime import datetime, timezone
from config import PipelineConfig

# schema
# เพิ่ม _ingest_ts (raw) และ _updated_ts (curated)
STUDENT_BASE_FIELDS: list[SchemaField] = [
    SchemaField("code", "STRING", mode="REQUIRED"),
    SchemaField("title", "STRING"),
    SchemaField("first_name_en", "STRING"),
    SchemaField("middle_name_en", "STRING"),
    SchemaField("last_name_en", "STRING"),
    SchemaField("gender", "STRING"),
    SchemaField("nationality", "STRING"),
    SchemaField("resident_type", "STRING"),
    SchemaField("student_fee_type", "STRING"),
    SchemaField("academic_year", "INT64"),
    SchemaField("academic_term", "STRING"),
    SchemaField("admission_type", "STRING"),
    SchemaField("student_type", "STRING"),
    SchemaField("student_status", "STRING"),
    SchemaField("major_code", "STRING"),
    SchemaField("major_name", "STRING"),
    SchemaField("division", "STRING"),
    SchemaField("division_name", "STRING"),
    SchemaField("is_active", "BOOL"),
    SchemaField("ingestion_date", "DATE"),
    SchemaField("created_at", "TIMESTAMP"),
    SchemaField("updated_at", "TIMESTAMP"),
]

RAW_SCHEMA: list[SchemaField] = [
    *STUDENT_BASE_FIELDS,
    SchemaField("_ingest_ts", "TIMESTAMP"),
]

CURATED_SCHEMA = [
    # same RAW but add _updated_ts
    *STUDENT_BASE_FIELDS,
    SchemaField("_updated_ts", "TIMESTAMP"),
]

# change tracking
TRACKED_FIELDS: list[str] = [
    "title",
    "first_name_en",
    "middle_name_en",
    "last_name_en",
    "gender",
    "nationality",
    "resident_type",
    "student_fee_type",
    "admission_type",
    "student_type",
    "student_status",
    "major_code",
    "major_name",
    "division",
    "division_name",
    "is_active",
]

# client
@contextmanager
def bq_client(cfg: PipelineConfig):
    if cfg.bq_key_path:
        client = bigquery.Client.from_service_account_json(str(cfg.bq_key_path),project=cfg.bq_project)
    else:
        client = bigquery.Client(project=cfg.bq_project)
    try:
        yield client
    finally:
        client.close()

# raw
def append_raw(client: bigquery.Client, cfg: PipelineConfig, df: pd.DataFrame) -> int:
    """
    Append normalized rows into the raw history table.
    """
    if df.empty:
        return 0
    
    df2 = df.copy()
    df2["_ingest_ts"] = datetime.now(timezone.utc)
    
    raw_fqn = f"{cfg.bq_project}.{cfg.bq_dataset}.{cfg.bq_raw_table}"
    
    job_cfg = LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=RAW_SCHEMA,
        time_partition=TimePartitioning(
            type_=TimePartitioningType.DAY,
            field="_ingest_ts", # แนะนำให้ partition ด้วย event-time ที่แน่นอน
        ),
    )
    
    client.load_table_from_dataframe(df2, raw_fqn, job_config=job_cfg).result()
    return len(df2)

# staging
def load_staging(client: bigquery.Client, cfg: PipelineConfig, df: pd.DataFrame) -> str:
    """
    Write the frame into a staging table and return table id (temporary)
    """
    if df.empty:
        # return empty to caller cross merge
        return ""
    
    stage_name = (
        f"{cfg.bq_project}.{cfg.bq_dataset}.{cfg.bq_table}"
        f"__stg_{datetime.now(timezone.utc):%Y%m%d%H%M%S}"
    )
    
    job_cfg = LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=CURATED_SCHEMA, # use same schema curated (add _updated_ts -> merge)
    )
    client.load_table_from_dataframe(df, stage_name, job_config=job_cfg).result()
    
    return stage_name

# change log
def fetch_change_log(client: bigquery.Client, cfg: PipelineConfig, stg_table: str) -> list[dict]:
    """
    Return a list of difference between staging rows and current curated rows.
    ใช้ IS DISTINCT FROM เพื่อจับ null-safe difference
    find field-level change between curated with staging (column important only)
    """
    
    if not stg_table:
        return []
    
    # ถ้า curated ยังไม่เคยมีให้ไม่มี change-log
    target_fqn = f"{cfg.bq_project}.{cfg.bq_dataset}.{cfg.bq_table}"
    try:
        client.get_table(target_fqn)
    except Exception:
        return []
    
    base_target = f"`{target_fqn}`"
    comparisons = list[str] = []
    for column in TRACKED_FIELDS:
        comparisons.append(
            f"""
                SELECT
                    s.code AS student_id,
                    '{column}' AS field,
                    CAST(t.{column} AS STRING) AS old_value,
                    CAST(s.{column} AS STRING) AS new_value
                FROM {base_target} AS t
                RIGHT JOIN `{stg_table}` AS s
                    ON t.code = s.code
                AND CAST(t.academic_year AS INT64) = CAST(s.academic_year AS INT64)
                AND CAST(t.academic_term AS STRING) = CAST(s.academic_term AS STRING)
                WHERE CAST(s.{column} AS STRING) IS DISTINCT FROM CAST(t.{column} AS STRING)
            """
        )
    change_sql = "UNION ALL ".join(comparisons)
    job = client.query(change_sql)
    
    return [dict(row) for row in job.result()]

def _bootstrap_curated_if_need(client: bigquery.Client, cfg: PipelineConfig) -> None:
    """Create curated table if not exists and set partition/cluster"""
    curated_fqn = f"{cfg.bq_project}.{cfg.bq_dataset}.{cfg.bq_table}"
    # CREATE TABLE IF NOT EXISTS … PARTITION BY DATE(_updated_ts) CLUSTER BY …
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{curated_fqn}` (
            code STRING,
            title STRING,
            first_name_en STRING,
            middle_name_en STRING,
            last_name_en STRING,
            gender STRING,
            nationality STRING,
            resident_type STRING,
            student_fee_type STRING,
            academic_year INT64,
            academic_term STRING,
            admission_type STRING,
            student_type STRING,
            student_status STRING,
            major_code STRING,
            major_name STRING,
            division STRING,
            division_name STRING,
            is_active BOOL,
            ingestion_date DATE,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            _updated_ts TIMESTAMP
        )
        PARTITION BY DATE(_updated_ts)
        CLUSTER BY code, academic_year, student_status
    """
    client.query(create_sql).result()


def merge_to_curated(client: bigquery.Client, cfg: PipelineConfig, stg_table:str):
    """Merge the staging table into the curated destination (Idempotent)."""
    if not stg_table:
        return {"job_id": None, "inserted": 0, "updated": 0, "deleted": 0}
    
    _bootstrap_curated_if_need(client, cfg)
    
    curated_fqn = f"{cfg.bq_project}.{cfg.bq_dataset}.{cfg.bq_table}"
    
    """
    note : store business timestamps (created_at/updated_at) from source
    and record system update time separate at __updated_ts
    """
    merge_sql = f"""
        MERGE `{curated_fqn}` AS t
        USING (
            SELECT * EXCEPT(rn)
            FROM (
                SELECT
                    s.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.code, s.academic_year, s.academic_term
                        ORDER BY COALESCE(s.updated_at, s.created_at) DESC,
                                 s.created_at DESC
                    ) AS rn
                FROM `{stg_table}` AS s
            )
            WHERE rn = 1
        ) AS s
        ON  t.code          = s.code
        AND t.academic_year = CAST(s.academic_year AS INT64)
        AND t.academic_term = CAST(s.academic_term AS STRING)

        WHEN MATCHED AND (
            (s.title           IS NOT NULL AND s.title           != ''  AND s.title           != t.title) OR
            (s.first_name_en   IS NOT NULL AND s.first_name_en   != ''  AND s.first_name_en   != t.first_name_en) OR
            (s.middle_name_en  IS DISTINCT FROM t.middle_name_en) OR
            (s.last_name_en    IS NOT NULL AND s.last_name_en    != ''  AND s.last_name_en    != t.last_name_en) OR
            (s.gender          IS NOT NULL AND s.gender          != ''  AND s.gender          != t.gender) OR
            (s.nationality     IS NOT NULL AND s.nationality     != ''  AND s.nationality     != t.nationality) OR
            (s.resident_type   IS NOT NULL AND s.resident_type   != ''  AND s.resident_type   != t.resident_type) OR
            (s.student_fee_type IS NOT NULL AND s.student_fee_type != '' AND s.student_fee_type != t.student_fee_type) OR
            (s.admission_type  IS NOT NULL AND s.admission_type  != ''  AND s.admission_type  != t.admission_type) OR
            (s.student_type    IS NOT NULL AND s.student_type    != ''  AND s.student_type    != t.student_type) OR
            (s.student_status  IS NOT NULL AND s.student_status  != ''  AND s.student_status  != t.student_status) OR
            (s.major_code      IS NOT NULL AND s.major_code      != ''  AND s.major_code      != t.major_code) OR
            (s.major_name      IS NOT NULL AND s.major_name      != ''  AND s.major_name      != t.major_name) OR
            (s.division        IS NOT NULL AND s.division        != ''  AND s.division        != t.division) OR
            (s.division_name   IS NOT NULL AND s.division_name   != ''  AND s.division_name   != t.division_name) OR
            (s.is_active       IS NOT NULL AND CAST(s.is_active AS BOOL) IS DISTINCT FROM t.is_active) OR
            (s.ingestion_date  IS NOT NULL AND CAST(s.ingestion_date AS DATE) IS DISTINCT FROM t.ingestion_date)
        )
        THEN UPDATE SET
            title            = IF(s.title IS NULL OR s.title = '', t.title, s.title),
            first_name_en    = IF(s.first_name_en IS NULL OR s.first_name_en = '', t.first_name_en, s.first_name_en),
            middle_name_en   = IF(s.middle_name_en IS NULL, t.middle_name_en, s.middle_name_en),
            last_name_en     = IF(s.last_name_en IS NULL OR s.last_name_en = '', t.last_name_en, s.last_name_en),
            gender           = IF(s.gender IS NULL OR s.gender = '', t.gender, s.gender),
            nationality      = IF(s.nationality IS NULL OR s.nationality = '', t.nationality, s.nationality),
            resident_type    = IF(s.resident_type IS NULL OR s.resident_type = '', t.resident_type, s.resident_type),
            student_fee_type = IF(s.student_fee_type IS NULL OR s.student_fee_type = '', t.student_fee_type, s.student_fee_type),
            admission_type   = IF(s.admission_type IS NULL OR s.admission_type = '', t.admission_type, s.admission_type),
            student_type     = IF(s.student_type IS NULL OR s.student_type = '', t.student_type, s.student_type),
            student_status   = IF(s.student_status IS NULL OR s.student_status = '', t.student_status, s.student_status),
            major_code       = IF(s.major_code IS NULL OR s.major_code = '', t.major_code, s.major_code),
            major_name       = IF(s.major_name IS NULL OR s.major_name = '', t.major_name, s.major_name),
            division         = IF(s.division IS NULL OR s.division = '', t.division, s.division),
            division_name    = IF(s.division_name IS NULL OR s.division_name = '', t.division_name, s.division_name),
            is_active        = IF(s.is_active IS NULL, t.is_active, CAST(s.is_active AS BOOL)),
            ingestion_date   = IF(s.ingestion_date IS NULL, t.ingestion_date, CAST(s.ingestion_date AS DATE)),
            created_at       = COALESCE(CAST(s.created_at AS TIMESTAMP), t.created_at),
            updated_at       = COALESCE(CAST(s.updated_at AS TIMESTAMP), t.updated_at),
            _updated_ts      = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN
        INSERT (
            code, title, first_name_en, middle_name_en, last_name_en,
            gender, nationality, resident_type, student_fee_type,
            academic_year, academic_term, admission_type, student_type,
            student_status, major_code, major_name, division, division_name,
            is_active, ingestion_date, created_at, updated_at, _updated_ts
        )
        VALUES (
            s.code, s.title, s.first_name_en, s.middle_name_en, s.last_name_en,
            s.gender, s.nationality, s.resident_type, s.student_fee_type,
            CAST(s.academic_year AS INT64),
            CAST(s.academic_term  AS STRING),
            s.admission_type, s.student_type, s.student_status,
            s.major_code, s.major_name, s.division, s.division_name,
            CAST(s.is_active AS BOOL),
            CAST(s.ingestion_date AS DATE),
            CAST(s.created_at AS TIMESTAMP),
            CAST(s.updated_at AS TIMESTAMP),
            CURRENT_TIMESTAMP()
        )
    """
    job = client.query(merge_sql).result()
    
    # DML stats (inserted/ updated/ deleted)
    props = getattr(job, "_properties", {}) or {}
    query_stats = props.get("statistics", {}).get("query", {})
    dml_stats = query_stats.get("dmlStats", {}) if query_stats.get("statementType") == "MERGE" else {}
    return {
        "job_id": job.job_id,
        "inserted": int(dml_stats.get("insertedRowCount", 0)),
        "updated": int(dml_stats.get("updatedRowCount", 0)),
        "deleted": int(dml_stats.get("deletedRowCount", 0)),
    }


# GC UTILS
def delete_table(client: bigquery.Client, table_name: str) -> None:
    """Remove a table. Missing tables are ignored."""
    client.delete_table(table_name, not_found_ok=True)
