from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Tuple, Callable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.mssql import NVARCHAR as MSSQL_NVARCHAR

logger = logging.getLogger("airflow.task")
AuditWriter = Optional[Callable[[dict], None]]


# -------------------------
# utils
# -------------------------
def chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]

def build_in_params(keys: List[str]) -> Tuple[str, dict]:
    placeholders, params = [], {}
    for i, k in enumerate(keys):
        pname = f"k{i}"
        placeholders.append(f":{pname}")
        params[pname] = k
    return ",".join(placeholders), params

def get_table_columns(engine, schema: str, table: str) -> list[str]:
    sql = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA=:s AND TABLE_NAME=:t
    ORDER BY ORDINAL_POSITION
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"s": schema, "t": table}).fetchall()
    return [r[0] for r in rows]

# -------------------------
# temp upload
# -------------------------
def upload_temp_table(df: pd.DataFrame, tgt_engine, schema: str, temp_table: str, key_col: str):
    """
    อัปโหลด df ไป temp table (replace)
    """
    # dtype_map: ทำแบบยืดหยุ่นสำหรับ staging_student (คอลัมน์เยอะ)
    dtype_map = {c: MSSQL_NVARCHAR(None) for c in df.columns}  # NVARCHAR(MAX)
    if key_col in dtype_map:
        dtype_map[key_col] = MSSQL_NVARCHAR(255)

    # optional: ถ้ามี _row_order ให้แคบลง
    if "_row_order" in dtype_map:
        dtype_map["_row_order"] = MSSQL_NVARCHAR(50)

    df.to_sql(
        name=temp_table,
        con=tgt_engine,
        schema=schema,
        if_exists="replace",
        index=False,
        dtype=dtype_map
    )
    logger.info("📤 Uploaded temp table %s.%s rows=%s", schema, temp_table, len(df))


# -------------------------
# ensure target table (simple + robust)
# -------------------------
def ensure_target_table(conn, schema: str, table: str, df_columns: List[str], key_col: str):
    """
    สร้างตารางเป้าหมายถ้ายังไม่มี
    - PK clustered ที่ key_col
    - ทุกคอลัมน์เป็น NVARCHAR(MAX) (ยกเว้น key เป็น NVARCHAR(255))
    """
    target_fqn = f"[{schema}].[{table}]"

    exists = conn.execute(text("""
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA=:s AND TABLE_NAME=:t
    """), {"s": schema, "t": table}).scalar()

    if not exists:
        cols = []
        cols.append(f"[{key_col}] NVARCHAR(255) NOT NULL")
        for c in df_columns:
            if c == key_col:
                continue
            cols.append(f"[{c}] NVARCHAR(MAX) NULL")

        cols_sql = ",\n                ".join(cols)
        conn.execute(text(f"""
            CREATE TABLE {target_fqn} (
                {cols_sql},
                CONSTRAINT PK_{table}_{key_col} PRIMARY KEY CLUSTERED ([{key_col}] ASC)
            );
        """))
        logger.info("🧱 Created target table %s", target_fqn)
        return

    # ถ้ามีตารางแล้ว: ensure key col เป็น NOT NULL + length พอทำ index
    row = conn.execute(text("""
        SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=:s AND TABLE_NAME=:t AND COLUMN_NAME=:c
    """), {"s": schema, "t": table, "c": key_col}).mappings().first()

    if row:
        dt = (row["DATA_TYPE"] or "").upper()
        maxlen = row["CHARACTER_MAXIMUM_LENGTH"]
        nullable = (row["IS_NULLABLE"] or "").upper() == "YES"
        if dt != "NVARCHAR" or maxlen in (None, -1) or nullable:
            logger.info("🔧 Altering key col %s -> NVARCHAR(255) NOT NULL", key_col)
            conn.execute(text(f"""
                DELETE FROM {target_fqn}
                WHERE [{key_col}] IS NULL OR LTRIM(RTRIM([{key_col}]))='';
            """))
            conn.execute(text(f"ALTER TABLE {target_fqn} ALTER COLUMN [{key_col}] NVARCHAR(255) NOT NULL;"))
    else:
        # key col หาย -> add
        conn.execute(text(f"ALTER TABLE {target_fqn} ADD [{key_col}] NVARCHAR(255) NULL;"))


# -------------------------
# merge one batch + audit diff (เหมือน finance invoice)
# -------------------------
def merge_batch_with_audit(
    conn,
    temp_fqn: str,
    target_fqn: str,
    key_col: str,
    batch_keys: List[str],
    compare_cols: List[str],
    audit_writer: AuditWriter = None,
) -> Tuple[int, int, List[dict]]:
    if not batch_keys:
        return 0, 0, []

    in_clause, params = build_in_params(batch_keys)

    # สร้างเงื่อนไข compare แบบ null-safe
    diff_conditions = " OR ".join([f"ISNULL(T.[{c}], '') <> ISNULL(S.[{c}], '')" for c in compare_cols])

    # update set clause
    update_clause = ",\n        ".join([f"T.[{c}] = S.[{c}]" for c in compare_cols])

    # insert columns / values
    insert_cols = ", ".join([f"[{key_col}]"] + [f"[{c}]" for c in compare_cols])
    insert_vals = ", ".join([f"S.[{key_col}]"] + [f"S.[{c}]" for c in compare_cols])

    # OUTPUT: เก็บ old/new เฉพาะคอลัมน์ที่สนใจ (compare_cols)
    output_cols = ",\n        ".join(
        [f"deleted.[{c}] AS old_{c}, inserted.[{c}] AS new_{c}" for c in compare_cols]
    )

    merge_sql = f"""
    SET NOCOUNT ON;

    DECLARE @audit TABLE(
      action NVARCHAR(10),
      [{key_col}] NVARCHAR(255),
      {", ".join([f"old_{c} NVARCHAR(MAX), new_{c} NVARCHAR(MAX)" for c in compare_cols])}
    );

    MERGE {target_fqn} AS T
    USING (
      SELECT * FROM {temp_fqn} WITH (NOLOCK)
      WHERE [{key_col}] IN ({in_clause})
    ) AS S
    ON T.[{key_col}] = S.[{key_col}]

    WHEN MATCHED AND ({diff_conditions}) THEN
      UPDATE SET
        {update_clause}

    WHEN NOT MATCHED BY TARGET THEN
      INSERT ({insert_cols})
      VALUES ({insert_vals})

    OUTPUT
      $action,
      inserted.[{key_col}],
      {output_cols}
    INTO @audit;

    SELECT * FROM @audit;
    """

    result = conn.execution_options(stream_results=True).execute(text(merge_sql), params)

    inserted_count, updated_count = 0, 0
    updated_samples: List[dict] = []

    for row in result.mappings():
        action = (row.get("action") or "").upper()
        if action == "INSERT":
            inserted_count += 1
        elif action == "UPDATE":
            updated_count += 1
            changes = {}
            for c in compare_cols:
                old_v = row.get(f"old_{c}")
                new_v = row.get(f"new_{c}")
                if str(old_v or "").strip() != str(new_v or "").strip():
                    changes[c] = {"old": old_v, "new": new_v}

            if changes:
                payload = {
                    "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "action": "UPDATE",
                    "studentCode": row.get(key_col),  # สำหรับ staging_student key เป็น studentCode
                    "changes": changes,
                }
                updated_samples.append(payload)
                if audit_writer:
                    audit_writer(payload)

    return inserted_count, updated_count, updated_samples


# -------------------------
# merge all batches
# -------------------------
def merge_all_in_batches(
    tgt_engine,
    schema: str,
    target_table: str,
    key_col: str,
    batch_size: int,
    audit_writer: AuditWriter = None,
    temp_table: Optional[str] = None,
    max_update_samples: int = 50,
) -> Tuple[int, int, List[dict]]:
    """
    - อ่าน keys จาก temp table
    - MERGE ต่อ batch
    - คืน inserted_total, updated_total, updated_samples
    """
    temp_table = temp_table or f"{target_table}_tmp"
    temp_fqn = f"[{schema}].[{temp_table}]"
    target_fqn = f"[{schema}].[{target_table}]"

    # ดึงคีย์ทั้งหมดจาก temp
    with tgt_engine.connect() as conn:
        rows = conn.execute(text(f"SELECT [{key_col}] FROM {temp_fqn}")).fetchall()
    keys = [r[0] for r in rows if r and r[0] is not None]

    if not keys:
        logger.info("⚠️ No keys in temp table %s", temp_fqn)
        return 0, 0, []

    # compare columns = ทุกคอลัมน์ใน temp ยกเว้น key และ _row_order
    with tgt_engine.connect() as conn:
        cols = conn.execute(text(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=:s AND TABLE_NAME=:t
            ORDER BY ORDINAL_POSITION
        """), {"s": schema, "t": temp_table}).fetchall()
        
    # อ่านคอลัมน์จริงจาก temp และ target
    temp_cols = get_table_columns(tgt_engine, schema, temp_table)
    target_cols = get_table_columns(tgt_engine, schema, target_table)

    # ✅ merge เฉพาะคอลัมน์ที่ target มีจริง
    compare_cols = [
        c for c in temp_cols
        if c in target_cols and c not in (key_col, "_row_order")
    ]

    # (ช่วย debug)
    excluded = [
        c for c in temp_cols
        if c not in compare_cols and c not in (key_col, "_row_order")
    ]
    logger.info("Excluded columns (not in target): %s", excluded)

    batches = chunk_list(keys, batch_size)
    inserted_total, updated_total = 0, 0
    updated_samples: List[dict] = []

    for i, batch in enumerate(batches, start=1):
        logger.info("➡️ MERGE batch %s/%s (batch_size=%s)", i, len(batches), batch_size)
        with tgt_engine.begin() as conn:
            ins, upd, upd_rows = merge_batch_with_audit(
                conn=conn,
                temp_fqn=temp_fqn,
                target_fqn=target_fqn,
                key_col=key_col,
                batch_keys=batch,
                compare_cols=compare_cols,
                audit_writer=audit_writer,
            )
        inserted_total += ins
        updated_total += upd

        if upd_rows and len(updated_samples) < max_update_samples:
            updated_samples.extend(upd_rows[: max_update_samples - len(updated_samples)])

        if audit_writer:
            audit_writer({
                "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "action": "BATCH_SUMMARY",
                "batch": i,
                "inserted": ins,
                "updated": upd,
            })

    return inserted_total, updated_total, updated_samples


# -------------------------
# drop temp
# -------------------------
def drop_temp_table(conn, schema: str, temp_table: str):
    conn.execute(text(
        f"IF OBJECT_ID(N'{schema}.{temp_table}', 'U') IS NOT NULL "
        f"DROP TABLE [{schema}].[{temp_table}];"
    ))