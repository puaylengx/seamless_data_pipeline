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
    return [items[i : i + size] for i in range(0, len(items), size)]


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
def upload_temp_table(
    df: pd.DataFrame, tgt_engine, schema: str, temp_table: str, key_col: str
):
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
        dtype=dtype_map,
    )
    logger.info("📤 Uploaded temp table %s.%s rows=%s", schema, temp_table, len(df))


# -------------------------
# ensure target table (simple + robust)
# -------------------------
def ensure_target_table(
    conn, schema: str, table: str, df_columns: List[str], key_col: str
):
    """
    สร้างตารางเป้าหมายถ้ายังไม่มี
    - PK clustered ที่ key_col
    - ทุกคอลัมน์เป็น NVARCHAR(MAX) (ยกเว้น key เป็น NVARCHAR(255))
    """
    target_fqn = f"[{schema}].[{table}]"

    exists = conn.execute(
        text("""
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA=:s AND TABLE_NAME=:t
    """),
        {"s": schema, "t": table},
    ).scalar()

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
    row = (
        conn.execute(
            text("""
        SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=:s AND TABLE_NAME=:t AND COLUMN_NAME=:c
    """),
            {"s": schema, "t": table, "c": key_col},
        )
        .mappings()
        .first()
    )

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
            conn.execute(
                text(
                    f"ALTER TABLE {target_fqn} ALTER COLUMN [{key_col}] NVARCHAR(255) NOT NULL;"
                )
            )
    else:
        # key col หาย -> add
        conn.execute(
            text(f"ALTER TABLE {target_fqn} ADD [{key_col}] NVARCHAR(255) NULL;")
        )


# -------------------------
# merge one batch + audit diff (เหมือน finance invoice)
# -------------------------
from typing import List, Tuple, Optional
from datetime import datetime
from sqlalchemy import text


def merge_batch_with_audit(
    conn,
    temp_fqn: str,
    target_fqn: str,
    key_col_temp: str,
    key_col_target: str,
    batch_keys: List[str],
    compare_pairs: List[Tuple[str, str]],  # (temp_col, target_col)
    audit_writer: AuditWriter = None,
) -> Tuple[int, int, List[dict]]:
    """
    ✅ FIX:
    - รองรับชื่อคอลัมน์ temp/target ที่ case ไม่ตรงกัน ด้วย compare_pairs
    - กัน NULL overwrite: update เฉพาะเมื่อ S.col IS NOT NULL และใช้ COALESCE(S, T)
    - diff compare แบบ type-safe ด้วย CONVERT(NVARCHAR(MAX), ...)
    """
    if not batch_keys:
        return 0, 0, []

    in_clause, params = build_in_params(batch_keys)

    # helper: type-safe compare string
    def _to_text(alias: str, col: str) -> str:
        return f"COALESCE(CONVERT(NVARCHAR(MAX), {alias}.[{col}]), '')"

    # --- diff_conditions: update เฉพาะกรณี source มีค่า และต่างจาก target ---
    # (กัน NULL จาก S ไปทับค่าเดิมใน T)
    diff_conditions = (
        " OR ".join(
            [
                f"(S.[{src}] IS NOT NULL AND {_to_text('T', tgt)} <> {_to_text('S', src)})"
                for (src, tgt) in compare_pairs
            ]
        )
        or "1 = 0"
    )  # ถ้าไม่มี compare_pairs จะไม่ update

    # --- update_clause: กัน NULL overwrite ด้วย COALESCE ---
    update_clause = (
        ",\n        ".join(
            [
                f"T.[{tgt}] = COALESCE(S.[{src}], T.[{tgt}])"
                for (src, tgt) in compare_pairs
            ]
        )
        or f"T.[{key_col_target}] = T.[{key_col_target}]"
    )  # กัน syntax error

    # --- insert columns / values: insert ลง target col แต่ดึงค่าจาก temp col ---
    insert_cols = ", ".join(
        [f"[{key_col_target}]"] + [f"[{tgt}]" for (_, tgt) in compare_pairs]
    )
    insert_vals = ", ".join(
        [f"S.[{key_col_temp}]"] + [f"S.[{src}]" for (src, _) in compare_pairs]
    )

    # --- OUTPUT: เก็บ old/new ของ target columns ---
    output_cols = (
        ",\n        ".join(
            [
                f"deleted.[{tgt}] AS old_{tgt}, inserted.[{tgt}] AS new_{tgt}"
                for (_, tgt) in compare_pairs
            ]
        )
        or "NULL AS old_dummy, NULL AS new_dummy"
    )

    audit_table_cols = ", ".join(
        [
            f"[old_{tgt}] NVARCHAR(MAX), [new_{tgt}] NVARCHAR(MAX)"
            for (_, tgt) in compare_pairs
        ]
    )

    merge_sql = f"""
    SET NOCOUNT ON;

    DECLARE @audit TABLE(
      action NVARCHAR(10),
      [{key_col_target}] NVARCHAR(255)
      {("," + audit_table_cols) if audit_table_cols else ""}
    );

    MERGE {target_fqn} AS T
    USING (
      SELECT * FROM {temp_fqn} WITH (NOLOCK)
      WHERE [{key_col_temp}] IN ({in_clause})
    ) AS S
    ON T.[{key_col_target}] = S.[{key_col_temp}]

    WHEN MATCHED AND ({diff_conditions}) THEN
      UPDATE SET
        {update_clause}

    WHEN NOT MATCHED BY TARGET THEN
      INSERT ({insert_cols})
      VALUES ({insert_vals})

    OUTPUT
      $action,
      inserted.[{key_col_target}],
      {output_cols}
    INTO @audit;

    SELECT * FROM @audit;
    """

    result = conn.execution_options(stream_results=True).execute(
        text(merge_sql), params
    )

    inserted_count, updated_count = 0, 0
    updated_samples: List[dict] = []

    for row in result.mappings():
        action = (row.get("action") or "").upper()
        if action == "INSERT":
            inserted_count += 1
        elif action == "UPDATE":
            updated_count += 1

            changes = {}
            for _, tgt in compare_pairs:
                old_v = row.get(f"old_{tgt}")
                new_v = row.get(f"new_{tgt}")
                if str(old_v or "").strip() != str(new_v or "").strip():
                    changes[tgt] = {"old": old_v, "new": new_v}

            if changes:
                payload = {
                    "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "action": "UPDATE",
                    # เก็บ key จาก target key col (ชื่อจริงใน target)
                    "studentCode": row.get(key_col_target),
                    "changes": changes,
                }
                updated_samples.append(payload)
                if audit_writer:
                    audit_writer(payload)

    return inserted_count, updated_count, updated_samples


# -------------------------
# merge all batches
# -------------------------
from typing import List, Tuple, Optional, Dict
from datetime import datetime
from sqlalchemy import text


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

    ✅ FIX:
    - ทำ column mapping แบบ case-insensitive (temp vs target)
    - ส่ง compare_pairs (temp_col, target_col) ให้ merge_batch_with_audit
    - รองรับ key_col ที่ชื่อไม่ตรง case กัน (temp_key vs target_key)
    """
    temp_table = temp_table or f"{target_table}_tmp"
    temp_fqn = f"[{schema}].[{temp_table}]"
    target_fqn = f"[{schema}].[{target_table}]"

    # อ่านคอลัมน์จริงจาก temp และ target
    temp_cols = get_table_columns(tgt_engine, schema, temp_table)
    target_cols = get_table_columns(tgt_engine, schema, target_table)

    # --- หา key column ที่ตรงจริงใน temp/target แบบ case-insensitive ---
    temp_key = next((c for c in temp_cols if c.lower() == key_col.lower()), key_col)
    target_key = next((c for c in target_cols if c.lower() == key_col.lower()), key_col)

    # --- ดึงคีย์ทั้งหมดจาก temp โดยใช้ temp_key ที่ถูกต้อง ---
    with tgt_engine.connect() as conn:
        rows = conn.execute(text(f"SELECT [{temp_key}] FROM {temp_fqn}")).fetchall()
    keys = [r[0] for r in rows if r and r[0] is not None]

    if not keys:
        logger.info("⚠️ No keys in temp table %s", temp_fqn)
        return 0, 0, []

    # --- สร้าง mapping คอลัมน์แบบ case-insensitive: temp -> target ---
    target_map: Dict[str, str] = {c.lower(): c for c in target_cols}

    compare_pairs: List[Tuple[str, str]] = []
    excluded: List[str] = []

    for c in temp_cols:
        if c.lower() in {temp_key.lower(), "_row_order"}:
            continue
        tgt_c = target_map.get(c.lower())
        if tgt_c:
            compare_pairs.append((c, tgt_c))  # (source/temp col, target col)
        else:
            excluded.append(c)

    logger.info("Excluded columns (not in target): %s", excluded)
    logger.info("Compare pairs (temp->target): %s", compare_pairs[:20])

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
                key_col_temp=temp_key,
                key_col_target=target_key,
                batch_keys=batch,
                compare_pairs=compare_pairs,
                audit_writer=audit_writer,
            )

        inserted_total += ins
        updated_total += upd

        if upd_rows and len(updated_samples) < max_update_samples:
            updated_samples.extend(
                upd_rows[: max_update_samples - len(updated_samples)]
            )

        if audit_writer:
            audit_writer(
                {
                    "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "action": "BATCH_SUMMARY",
                    "batch": i,
                    "inserted": ins,
                    "updated": upd,
                }
            )

    return inserted_total, updated_total, updated_samples


# -------------------------
# drop temp
# -------------------------
def drop_temp_table(conn, schema: str, temp_table: str):
    conn.execute(
        text(
            f"IF OBJECT_ID(N'{schema}.{temp_table}', 'U') IS NOT NULL "
            f"DROP TABLE [{schema}].[{temp_table}];"
        )
    )
