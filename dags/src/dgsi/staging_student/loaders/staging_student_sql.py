from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Tuple, Callable, Optional, Dict

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
    df: pd.DataFrame,
    tgt_engine,
    schema: str,
    temp_table: str,
    key_col: str,
):
    """
    อัปโหลด df ไป temp table (replace)
    - ทุกคอลัมน์ NVARCHAR(MAX)
    - key เป็น NVARCHAR(255)
    """
    dtype_map = {c: MSSQL_NVARCHAR(None) for c in df.columns}  # NVARCHAR(MAX)
    if key_col in dtype_map:
        dtype_map[key_col] = MSSQL_NVARCHAR(255)

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
    conn,
    schema: str,
    table: str,
    df_columns: List[str],
    key_col: str,
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
        cols = [f"[{key_col}] NVARCHAR(255) NOT NULL"]
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

            # ลบ key ที่ null/ว่างก่อน
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
# merge one batch + audit diff (FIXED)
# -------------------------
def merge_batch_with_audit(
    conn,
    temp_fqn: str,
    target_fqn: str,
    key_col_temp: str,
    key_col_target: str,
    batch_keys: List[str],
    compare_pairs: List[Tuple[str, str]],  # (src/temp, tgt/target)
    audit_writer: AuditWriter = None,
) -> Tuple[int, int, List[dict]]:
    """
    MERGE ต่อ batch + audit เฉพาะ update ที่ "ต่างจริง"
    FIX:
    - normalize compare (trim/lower/replace NBSP+tab)
    - กัน NULL/ว่าง ไม่ให้ overwrite
    - นับ UPDATE เฉพาะที่มี changes จริง
    """

    if not batch_keys:
        return 0, 0, []

    in_clause, params = build_in_params(batch_keys)

    # normalize expression ใน SQL (กัน whitespace แปลกๆ)
    def _norm(alias: str, col: str) -> str:
        # NBSP (CHAR(160)) -> space, TAB -> space, trim, empty->''
        return f"""
        LOWER(
          COALESCE(
            NULLIF(
              LTRIM(RTRIM(
                REPLACE(REPLACE(CONVERT(NVARCHAR(MAX), {alias}.[{col}]), CHAR(160), ' '), CHAR(9), ' ')
              )),
            ''),
          '')
        )
        """

    # clean value สำหรับเขียนลง target (trim + replace NBSP/tab, empty->NULL)
    def _clean_value(alias: str, col: str) -> str:
        return f"""
        NULLIF(
          LTRIM(RTRIM(
            REPLACE(REPLACE(CONVERT(NVARCHAR(MAX), {alias}.[{col}]), CHAR(160), ' '), CHAR(9), ' ')
          )),
        '')
        """

    # diff: update เฉพาะเมื่อ source "มีค่า" และ normalized ต่างกันจริง
    diff_conditions = (
        " OR ".join(
            [
                f"({_norm('S', src)} <> {_norm('T', tgt)} AND {_norm('S', src)} <> '')"
                for (src, tgt) in compare_pairs
            ]
        )
        or "1 = 0"
    )

    # update: ไม่ให้ NULL/empty overwrite + เขียนค่า clean แล้ว
    update_clause = (
        ",\n        ".join([f"""
                T.[{tgt}] = CASE
                  WHEN {_clean_value('S', src)} IS NULL THEN T.[{tgt}]
                  WHEN {_norm('S', src)} <> {_norm('T', tgt)} THEN {_clean_value('S', src)}
                  ELSE T.[{tgt}]
                END
                """.strip() for (src, tgt) in compare_pairs])
        or f"T.[{key_col_target}] = T.[{key_col_target}]"
    )

    # insert: ใส่ค่า clean แล้ว (กันรันแล้ว diff ซ้ำ)
    insert_cols = ", ".join(
        [f"[{key_col_target}]"] + [f"[{tgt}]" for (_, tgt) in compare_pairs]
    )
    insert_vals = ", ".join(
        [f"S.[{key_col_temp}]"]
        + [f"{_clean_value('S', src)}" for (src, _) in compare_pairs]
    )

    output_cols = (
        ",\n      ".join(
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

    MERGE {target_fqn} WITH (HOLDLOCK) AS T
    USING (
      SELECT *
      FROM {temp_fqn} WITH (NOLOCK)
      WHERE [{key_col_temp}] IN ({in_clause})
    ) AS S
    ON LTRIM(RTRIM(CONVERT(NVARCHAR(255), T.[{key_col_target}])))
       = LTRIM(RTRIM(CONVERT(NVARCHAR(255), S.[{key_col_temp}])))

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

    # normalize ใน Python ให้ตรงกับ SQL
    def _py_norm(v):
        if v is None:
            return ""
        return str(v).replace("\u00a0", " ").replace("\t", " ").strip().lower()

    for row in result.mappings():
        action = (row.get("action") or "").upper()

        if action == "INSERT":
            inserted_count += 1

        elif action == "UPDATE":
            # นับ update เฉพาะที่ต่างจริงหลัง normalize
            changes = {}
            for _, tgt in compare_pairs:
                old_v = row.get(f"old_{tgt}")
                new_v = row.get(f"new_{tgt}")
                if _py_norm(old_v) != _py_norm(new_v):
                    changes[tgt] = {"old": old_v, "new": new_v}

            if changes:
                updated_count += 1
                payload = {
                    "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "action": "UPDATE",
                    "studentCode": row.get(key_col_target),
                    "changes": changes,
                }
                updated_samples.append(payload)
                if audit_writer:
                    audit_writer(payload)

    return inserted_count, updated_count, updated_samples


# -------------------------
# merge all batches (FIXED mapping + ignore cols)
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
    ignore_cols: Optional[
        List[str]
    ] = None,  # ตัดคอลัมน์ที่เปลี่ยนทุกครั้ง เช่น updated_at, load_ts
) -> Tuple[int, int, List[dict]]:
    """
    - อ่าน keys จาก temp table
    - MERGE ต่อ batch
    - คืน inserted_total, updated_total, updated_samples

    FIX:
    - column mapping แบบ case-insensitive (temp vs target)
    - key col case-insensitive
    - ignore_cols เพื่อไม่ให้ update ซ้ำจากคอลัมน์ที่เปลี่ยนทุกรอบ
    """

    ignore_cols_set = set([c.lower() for c in (ignore_cols or [])])

    temp_table = temp_table or f"{target_table}_tmp"
    temp_fqn = f"[{schema}].[{temp_table}]"
    target_fqn = f"[{schema}].[{target_table}]"

    # อ่านคอลัมน์จริงจาก temp และ target
    temp_cols = get_table_columns(tgt_engine, schema, temp_table)
    target_cols = get_table_columns(tgt_engine, schema, target_table)

    # หา key จริงแบบ case-insensitive
    temp_key = next((c for c in temp_cols if c.lower() == key_col.lower()), key_col)
    target_key = next((c for c in target_cols if c.lower() == key_col.lower()), key_col)

    # ดึง keys จาก temp (trim ลด mismatch)
    with tgt_engine.connect() as conn:
        rows = conn.execute(text(f"SELECT [{temp_key}] FROM {temp_fqn}")).fetchall()

    keys: List[str] = []
    for r in rows:
        if not r or r[0] is None:
            continue
        k = str(r[0]).strip()
        if k:
            keys.append(k)

    if not keys:
        logger.info("⚠️ No keys in temp table %s", temp_fqn)
        return 0, 0, []

    # mapping target col lower -> real col
    target_map: Dict[str, str] = {c.lower(): c for c in target_cols}

    compare_pairs: List[Tuple[str, str]] = []
    excluded: List[str] = []

    for c in temp_cols:
        lc = c.lower()
        if lc in {temp_key.lower(), "_row_order"}:
            continue
        if lc in ignore_cols_set:
            excluded.append(c)
            continue

        tgt_c = target_map.get(lc)
        if tgt_c:
            compare_pairs.append((c, tgt_c))  # (source/temp col, target col)
        else:
            excluded.append(c)

    logger.info("Excluded columns: %s", excluded)
    logger.info("Compare pairs (temp->target) sample: %s", compare_pairs[:20])

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
