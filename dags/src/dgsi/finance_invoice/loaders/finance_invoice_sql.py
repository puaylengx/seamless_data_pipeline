import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.mssql import NVARCHAR as MSSQL_NVARCHAR
from sqlalchemy.types import Integer, Numeric, DateTime
import logging
from typing import Callable, List, Tuple, Optional
from datetime import datetime

logger = logging.getLogger("airflow.task")
AuditWriter = Optional[Callable[[dict], None]]  # เช่น ฟังก์ชัน write_audit_line(payload)

# ─────────────────────────────── UTILS ───────────────────────────────
def chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i+size] for i in range(0, len(items), size)]

def build_in_params(keys: List[str]) -> Tuple[str, dict]:
    placeholders, params = [], {}
    for i, k in enumerate(keys):
        key = f"k{i}"
        placeholders.append(f":{key}")
        params[key] = k
    return ",".join(placeholders), params

def upload_temp_table(df: pd.DataFrame, tgt_engine, schema: str, temp_table: str):
    dtype_map = {
        "invoiceId": MSSQL_NVARCHAR(30),
        "acaYear": MSSQL_NVARCHAR(10),
        "semester": Integer(),
        "invoiceNo": MSSQL_NVARCHAR(50),
        "regisType": MSSQL_NVARCHAR(10),
        "invoiceAmount": Numeric(18, 2),
        "paidDate": DateTime(),
        "paidAmount": Numeric(18, 2),
        "paidStatus": MSSQL_NVARCHAR(5),
        "invoiceType": MSSQL_NVARCHAR(200),
        "schNameTh": MSSQL_NVARCHAR(200),
        "remark": MSSQL_NVARCHAR(400),
        "studentCode": MSSQL_NVARCHAR(50),
        "_row_order": MSSQL_NVARCHAR(50),
    }
    df.to_sql(temp_table,
        tgt_engine,
        schema=schema,
        if_exists="replace",
        index=False,
        dtype=dtype_map
    )
    
    logger.info("📤 Uploaded temp table %s.%s rows=%s", schema, temp_table, len(df))

def ensure_target_table(conn, schema: str, table: str):
    # ยก ensure_target_table เดิมมาใส่ตรงนี้
    target_fqn = f"[{schema}].[{table}]"

    exists = conn.execute(text("""
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA=:s AND TABLE_NAME=:t
    """), {"s": schema, "t": table}).scalar()

    if not exists:
        conn.execute(text(f"""
            CREATE TABLE {target_fqn} (
                invoiceId   NVARCHAR(30) NOT NULL,
                acaYear     NVARCHAR(10) NULL,
                semester    INT NULL,
                invoiceNo   NVARCHAR(50) NULL,
                regisType   NVARCHAR(10) NULL,
                invoiceAmount DECIMAL(18,2) NULL,
                paidDate    DATETIME NULL,
                paidAmount  DECIMAL(18,2) NULL,
                paidStatus  NVARCHAR(5) NULL,
                invoiceType NVARCHAR(200) NULL,
                schNameTh   NVARCHAR(200) NULL,
                remark      NVARCHAR(400) NULL,
                studentCode NVARCHAR(50) NULL,
                CONSTRAINT PK_{table}_invoiceId PRIMARY KEY CLUSTERED (invoiceId ASC)
            );
        """))
        logger.info(f"🧱 Created target table {target_fqn}")
        return

    row = conn.execute(text("""
        SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=:s AND TABLE_NAME=:t AND COLUMN_NAME='invoiceId'
    """), {"s": schema, "t": table}).mappings().first()

    if not row:
        conn.execute(text(f"ALTER TABLE {target_fqn} ADD [invoiceId] NVARCHAR(30) NULL;"))
        logger.info("➕ Added missing key column [invoiceId] NVARCHAR(30) to %s", target_fqn)
    else:
        dt = (row["DATA_TYPE"] or "").upper()
        maxlen = row["CHARACTER_MAXIMUM_LENGTH"]
        nullable = (row["IS_NULLABLE"] or "").upper() == "YES"
        if dt != "NVARCHAR" or maxlen in (None, -1) or nullable:
            logger.info(f"🔧 Altering [invoiceId] from {dt}({maxlen}) NULLABLE={nullable} → NVARCHAR(30) NOT NULL")
            conn.execute(text(f"""
                DELETE FROM {target_fqn}
                WHERE [invoiceId] IS NULL OR LTRIM(RTRIM([invoiceId]))='' OR [invoiceId] LIKE '%[^0-9]%';
            """))
            conn.execute(text(f"ALTER TABLE {target_fqn} ALTER COLUMN [invoiceId] NVARCHAR(30) NOT NULL;"))

    has_pk = conn.execute(text("""
        SELECT 1 FROM sys.key_constraints
        WHERE parent_object_id=OBJECT_ID(:fqn) AND type='PK'
    """), {"fqn": f"{schema}.{table}"}).scalar()

    if not has_pk:
        try:
            conn.execute(text(f"""
                ALTER TABLE {target_fqn}
                ADD CONSTRAINT PK_{table}_invoiceId PRIMARY KEY CLUSTERED (invoiceId ASC);
            """))
            logger.info(f"🔐 Added PK on {target_fqn}(invoiceId)")
        except Exception as e:
            logger.error("⚠️ Cannot add clustered PK; creating UNIQUE NONCLUSTERED instead.", e)
            conn.execute(text(f"""
                IF NOT EXISTS (
                  SELECT 1 FROM sys.indexes
                  WHERE object_id=OBJECT_ID('{schema}.{table}') AND is_unique=1 AND name='UX_{table}_invoiceId'
                )
                CREATE UNIQUE NONCLUSTERED INDEX UX_{table}_invoiceId ON {target_fqn}([invoiceId]);
            """))

def merge_batch_with_audit(conn,temp_fqn: str,target_fqn: str,batch_keys: List[str],audit_writer: AuditWriter = None,):
    # ยก merge_batch_with_audit + process_batches เดิมมา
    # คืน inserted_total, updated_total, updated_rows_samples
    in_clause, params = build_in_params(batch_keys)

    merge_sql = f"""
    SET NOCOUNT ON;

    DECLARE @audit TABLE(
        action NVARCHAR(10),
        invoiceId NVARCHAR(30),
        oldPaidDate DATETIME, newPaidDate DATETIME,
        oldPaidAmount DECIMAL(18,2), newPaidAmount DECIMAL(18,2),
        oldPaidStatus NVARCHAR(5), newPaidStatus NVARCHAR(5)
    );

    MERGE {target_fqn} AS T
    USING (
        SELECT * FROM {temp_fqn} WITH (NOLOCK)
        WHERE invoiceId IN ({in_clause})
    ) AS S
    ON T.invoiceId = S.invoiceId

    WHEN MATCHED AND (
           ISNULL(T.paidDate,'1900-01-01') <> ISNULL(S.paidDate,'1900-01-01')
        OR ISNULL(T.paidAmount,0) <> ISNULL(S.paidAmount,0)
        OR ISNULL(T.paidStatus,'') <> ISNULL(S.paidStatus,'')
        OR ISNULL(T.acaYear,'') <> ISNULL(S.acaYear,'')
        OR ISNULL(T.semester,0) <> ISNULL(S.semester,0)
        OR ISNULL(T.invoiceNo,'') <> ISNULL(S.invoiceNo,'')
        OR ISNULL(T.regisType,'') <> ISNULL(S.regisType,'')
        OR ISNULL(T.invoiceAmount,0) <> ISNULL(S.invoiceAmount,0)
        OR ISNULL(T.invoiceType,'') <> ISNULL(S.invoiceType,'')
        OR ISNULL(T.schNameTh,'') <> ISNULL(S.schNameTh,'')
        OR ISNULL(T.remark,'') <> ISNULL(S.remark,'')
        OR ISNULL(T.studentCode,'') <> ISNULL(S.studentCode,'')
    )
    THEN UPDATE SET
        T.acaYear       = S.acaYear,
        T.semester      = S.semester,
        T.invoiceNo     = S.invoiceNo,
        T.regisType     = S.regisType,
        T.invoiceAmount = S.invoiceAmount,
        T.paidDate      = S.paidDate,
        T.paidAmount    = S.paidAmount,
        T.paidStatus    = S.paidStatus,
        T.invoiceType   = S.invoiceType,
        T.schNameTh     = S.schNameTh,
        T.remark        = S.remark,
        T.studentCode   = S.studentCode

    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        invoiceId, acaYear, semester, invoiceNo, regisType,
        invoiceAmount, paidDate, paidAmount, paidStatus,
        invoiceType, schNameTh, remark, studentCode
      )
      VALUES (
        S.invoiceId, S.acaYear, S.semester, S.invoiceNo, S.regisType,
        S.invoiceAmount, S.paidDate, S.paidAmount, S.paidStatus,
        S.invoiceType, S.schNameTh, S.remark, S.studentCode
      )

    OUTPUT
        $action,
        inserted.invoiceId,
        deleted.paidDate, inserted.paidDate,
        deleted.paidAmount, inserted.paidAmount,
        deleted.paidStatus, inserted.paidStatus
    INTO @audit;

    SELECT * FROM @audit;
    """

    
    result = conn.execution_options(stream_results=True).execute(text(merge_sql), params)

    inserted_count, updated_count = 0, 0
    updated_payloads = []

    for row in result.mappings():
        action = (row.get("action") or "").upper()
        if action == "INSERT":
            inserted_count += 1
        elif action == "UPDATE":
            updated_count += 1
            changes = {}

            if row.get("oldPaidDate") != row.get("newPaidDate"):
                changes["paidDate"] = {"old": row.get("oldPaidDate"), "new": row.get("newPaidDate")}
            if str(row.get("oldPaidAmount")) != str(row.get("newPaidAmount")):
                changes["paidAmount"] = {"old": row.get("oldPaidAmount"), "new": row.get("newPaidAmount")}
            if row.get("oldPaidStatus") != row.get("newPaidStatus"):
                changes["paidStatus"] = {"old": row.get("oldPaidStatus"), "new": row.get("newPaidStatus")}

            if changes:
                payload = {
                    "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "action": "UPDATE",
                    "invoiceId": row.get("invoiceId"),
                    "changes": changes,
                }
                updated_payloads.append(payload)
                if audit_writer:
                    audit_writer(payload)

    return inserted_count, updated_count, updated_payloads


def merge_all_in_batches(
    tgt_engine,
    schema: str,
    target_table: str,
    key_col: str,
    batch_size: int,
    audit_writer: AuditWriter = None,
    temp_table: Optional[str] = None,
    max_update_samples: int = 50,
):
    """
    - อ่าน keys จาก temp table
    - MERGE เข้าตารางจริงเป็น batch
    - คืน inserted_total, updated_total, updated_rows_samples
    """
    if batch_size > 2000:
        logger.warning("batch_size=%s อาจชน SQL Server parameter limit (~2100). แนะนำ <= 2000", batch_size)

    temp_table = temp_table or f"{target_table}_tmp"
    temp_fqn = f"[{schema}].[{temp_table}]"
    target_fqn = f"[{schema}].[{target_table}]"

    # ดึง key ทั้งหมดจาก temp
    with tgt_engine.connect() as conn:
        key_rows = conn.execute(text(f"SELECT {key_col} FROM {temp_fqn}")).fetchall()
    keys = [r[0] for r in key_rows if r and r[0] is not None]

    total = len(keys)
    if total == 0:
        logger.info("⚠️ ไม่พบ key ใน temp table %s", temp_fqn)
        return 0, 0, []

    batches = chunk_list(keys, batch_size)
    inserted_total, updated_total = 0, 0
    updated_rows_samples = []

    for i, batch in enumerate(batches, start=1):
        start_row = (i - 1) * batch_size + 1
        end_row = min(i * batch_size, total)
        logger.info("➡️ MERGE batch %s/%s (rows %s-%s)", i, len(batches), start_row, end_row)

        with tgt_engine.begin() as conn:
            ins, upd, upd_rows = merge_batch_with_audit(
                conn=conn,
                temp_fqn=temp_fqn,
                target_fqn=target_fqn,
                batch_keys=batch,
                audit_writer=audit_writer,
            )

        inserted_total += ins
        updated_total += upd

        # เก็บ sample update ไว้ทำ email/report (จำกัดจำนวน)
        if upd_rows and len(updated_rows_samples) < max_update_samples:
            remain = max_update_samples - len(updated_rows_samples)
            updated_rows_samples.extend(upd_rows[:remain])

        # เขียน audit ต่อ batch (optional)
        if audit_writer:
            audit_writer({
                "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "action": "BATCH_SUMMARY",
                "batch": i,
                "rows_range": [start_row, end_row],
                "inserted": ins,
                "updated": upd,
            })

        logger.info("✅ MERGE batch %s done (INSERT=%s, UPDATE=%s)", i, ins, upd)

    return inserted_total, updated_total, updated_rows_samples


def drop_temp_table(conn, schema: str, temp_table: str):
    conn.execute(text(
        f"IF OBJECT_ID(N'{schema}.{temp_table}', 'U') IS NOT NULL "
        f"DROP TABLE [{schema}].[{temp_table}];"
    ))
