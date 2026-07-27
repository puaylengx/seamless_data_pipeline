#!/usr/bin/env python3
"""
โหลด Candidate-62_66.xlsx เข้า muic_enrollment.dbo.[candidate-62_66] แบบล้างแล้วใส่ใหม่

สคริปต์นี้ "ไม่ใช้ Airflow" — รันตรงจากเครื่องได้เลย ไม่ต้องมี /opt/airflow
    pip install pandas openpyxl sqlalchemy pymssql

    # 1) ตั้งค่าการเชื่อมต่อ (ก๊อปค่าจาก Airflow UI > Admin > Connections > mssql_enrollment)
    cp scripts/enrollment_db.env.example scripts/enrollment_db.env
    #    แล้วแก้ค่าในไฟล์ (ไฟล์นี้อยู่ใน .gitignore แล้ว)

    # 2) ดูก่อนว่าจะเกิดอะไรขึ้น (ไม่เขียน DB)
    python scripts/load_enrollment_excel_to_db.py

    # 3) เขียนจริง (backup ตารางเดิมอัตโนมัติ)
    python scripts/load_enrollment_excel_to_db.py --apply

จะส่งค่าทาง CLI แทนไฟล์ env ก็ได้:
    python scripts/load_enrollment_excel_to_db.py --host 10.0.0.5 --database muic_enrollment \
        --user sa --password 'xxx' --apply

ความปลอดภัย:
- default เป็น dry-run ต้องใส่ --apply ถึงจะเขียนจริง
- backup ตารางเดิมเป็น [<table>_bak_YYYYmmdd_HHMM] ก่อนเสมอ (ปิดด้วย --no-backup)
- ตรวจ student_id ซ้ำ + ตรวจคอลัมน์ให้ตรงกับตารางปลายทางก่อนเขียน
- truncate + insert อยู่ใน transaction เดียวกัน ถ้า insert พังจะ rollback ตารางกลับมาเหมือนเดิม
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger("load_enrollment_excel")

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_XLSX = REPO_ROOT / "dags" / "data" / "staging_student" / "Candidate-62_66.xlsx"
DEFAULT_ENV = Path(__file__).resolve().parent / "enrollment_db.env"
DEFAULT_SCHEMA = "dbo"
DEFAULT_TABLE = "candidate-62_66"
KEY_COL = "student_id"


def normalize(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


# ------------------------------------------------------------------
# การเชื่อมต่อ (ไม่พึ่ง Airflow)
# ------------------------------------------------------------------
def load_env_file(path: Path) -> None:
    """อ่านไฟล์ KEY=VALUE ใส่เข้า os.environ (ไม่ทับค่าที่ตั้งไว้แล้ว)"""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    logger.info("โหลดค่าเชื่อมต่อจาก %s", path)


def pick_driver(preferred: str | None) -> str:
    """เลือก DBAPI ที่ติดตั้งอยู่จริง — pymssql ติดตั้งง่ายกว่า (ไม่ต้องมี ODBC driver)"""
    available = []
    for mod, name in (("pymssql", "pymssql"), ("pyodbc", "pyodbc")):
        try:
            __import__(mod)
            available.append(name)
        except ImportError:
            pass

    if preferred:
        if preferred not in available:
            raise SystemExit(
                f"ไม่พบ {preferred} — ติดตั้งด้วย `pip install {preferred}` "
                f"(ที่ติดตั้งอยู่: {available or 'ไม่มีเลย'})"
            )
        return preferred

    if not available:
        raise SystemExit("ต้องติดตั้ง pymssql หรือ pyodbc ก่อน — แนะนำ `pip install pymssql`")
    return available[0]


def build_engine(args) -> tuple[object, str]:
    host = args.host or os.getenv("ENROLL_DB_HOST")
    port = args.port or os.getenv("ENROLL_DB_PORT") or "1433"
    database = args.database or os.getenv("ENROLL_DB_NAME") or "muic_enrollment"
    user = args.user or os.getenv("ENROLL_DB_USER")
    password = args.password or os.getenv("ENROLL_DB_PASSWORD")

    if args.dsn:
        return create_engine(args.dsn, pool_pre_ping=True), args.dsn.split("://")[0]

    missing = [n for n, v in (("host", host), ("user", user), ("password", password)) if not v]
    if missing:
        raise SystemExit(
            f"ขาดค่าเชื่อมต่อ: {', '.join(missing)} — ตั้งใน {DEFAULT_ENV.name} "
            f"หรือส่งทาง --host/--user/--password"
        )

    driver = pick_driver(args.driver)

    if driver == "pymssql":
        url = (
            f"mssql+pymssql://{quote_plus(user)}:{quote_plus(password)}"
            f"@{host}:{port}/{database}?charset=utf8"
        )
        engine = create_engine(url, pool_pre_ping=True)
    else:
        odbc_driver = os.getenv("ENROLL_DB_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
        odbc = (
            f"DRIVER={{{odbc_driver}}};"
            f"SERVER={host},{port};DATABASE={database};"
            f"UID={user};PWD={password};"
            f"Encrypt=no;TrustServerCertificate=yes;Connection Timeout=10;"
        )
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}",
            pool_pre_ping=True,
            fast_executemany=True,
        )

    logger.info("เชื่อมต่อ %s@%s:%s/%s ผ่าน %s", user, host, port, database, driver)
    return engine, driver


# ------------------------------------------------------------------
# อ่าน + ตรวจไฟล์ Excel
# ------------------------------------------------------------------
def read_excel(path: Path, sheet: int | str = 0) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    logger.info("อ่าน Excel: %s แถว x %s คอลัมน์ (%s)", len(df), len(df.columns), path.name)
    return df


def validate_excel(df: pd.DataFrame) -> None:
    """ตรวจก่อนเขียน: ต้องมี student_id, ต้องไม่ว่าง, ต้องไม่ซ้ำ"""
    key = next((c for c in df.columns if normalize(c) == KEY_COL), None)
    if key is None:
        raise ValueError(f"ไม่พบคอลัมน์ '{KEY_COL}' ในไฟล์ Excel (คอลัมน์ที่มี: {list(df.columns)[:10]} ...)")

    def _norm_id(v) -> str:
        if pd.isna(v):
            return ""
        s = str(v).strip()
        return s[:-2] if s.endswith(".0") else s

    ids = df[key].map(_norm_id)

    blank = ids.isin(["", "nan", "None"])
    if blank.any():
        raise ValueError(f"มี {KEY_COL} ว่าง {int(blank.sum())} แถว (แถวที่ {list(df.index[blank][:10] + 2)})")

    dup = ids[ids.duplicated(keep=False)]
    if not dup.empty:
        raise ValueError(
            f"มี {KEY_COL} ซ้ำ {dup.nunique()} คน ({len(dup)} แถว): {sorted(dup.unique())[:20]}"
        )

    logger.info("ตรวจไฟล์ผ่าน: %s ไม่ซ้ำ %s ค่า ✅", KEY_COL, ids.nunique())


# ------------------------------------------------------------------
# ข้อมูลตารางปลายทาง
# ------------------------------------------------------------------
def get_target_columns(conn, schema: str, table: str) -> list[dict]:
    rows = conn.execute(
        text("""
            SELECT c.COLUMN_NAME,
                   COLUMNPROPERTY(OBJECT_ID(QUOTENAME(:s) + '.' + QUOTENAME(:t)),
                                  c.COLUMN_NAME, 'IsIdentity') AS is_identity
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = :s AND c.TABLE_NAME = :t
            ORDER BY c.ORDINAL_POSITION
        """),
        {"s": schema, "t": table},
    ).mappings().all()
    if not rows:
        raise ValueError(f"ไม่พบตาราง [{schema}].[{table}] — ตรวจชื่อตาราง/สิทธิ์อีกครั้ง")
    return [{"name": r["COLUMN_NAME"], "identity": bool(r["is_identity"])} for r in rows]


def align_columns(df: pd.DataFrame, target_cols: list[dict]) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    จับคู่คอลัมน์ Excel กับคอลัมน์จริงในตาราง โดยเทียบแบบ normalize (lower + underscore)
    คืน (df ที่เปลี่ยนชื่อคอลัมน์เป็นชื่อจริงใน DB แล้ว, คอลัมน์ที่ Excel ไม่มี, คอลัมน์ที่ DB ไม่มี)
    """
    target_map = {normalize(c["name"]): c["name"] for c in target_cols}
    excel_map = {normalize(c): c for c in df.columns}

    # เรียงตามลำดับคอลัมน์จริงในตาราง เพื่อให้ dry-run อ่านง่ายและผลลัพธ์คงที่
    matched = {
        excel_map[normalize(c["name"])]: c["name"]
        for c in target_cols
        if normalize(c["name"]) in excel_map
    }
    missing_in_excel = [target_map[k] for k in target_map.keys() - excel_map.keys()]
    missing_in_db = [excel_map[k] for k in excel_map.keys() - target_map.keys()]

    out = df[list(matched)].rename(columns=matched)
    return out, sorted(missing_in_excel), sorted(missing_in_db)


def to_db_values(df: pd.DataFrame) -> pd.DataFrame:
    """NaN/ค่าว่าง -> None เพื่อให้เข้า DB เป็น NULL ไม่ใช่สตริง 'nan'"""
    out = df.copy()
    for c in out.columns:
        s = out[c].astype("object")
        out[c] = s.where(s.notna() & (s.astype(str).str.strip() != ""), None)
    return out


# ------------------------------------------------------------------
# เขียน DB
# ------------------------------------------------------------------
def backup_table(conn, schema: str, table: str) -> str:
    bak = f"{table}_bak_{datetime.now().strftime('%Y%m%d_%H%M')}"
    conn.execute(text(f"SELECT * INTO [{schema}].[{bak}] FROM [{schema}].[{table}];"))
    n = conn.execute(text(f"SELECT COUNT(*) FROM [{schema}].[{bak}]")).scalar()
    logger.info("🗄️  backup แล้ว -> [%s].[%s] (%s แถว)", schema, bak, n)
    return bak


def clear_table(conn, schema: str, table: str) -> None:
    """TRUNCATE ถ้าทำได้ (เร็วกว่า) ไม่ได้ค่อย DELETE"""
    fqn = f"[{schema}].[{table}]"
    try:
        conn.execute(text(f"TRUNCATE TABLE {fqn};"))
        logger.info("🧹 TRUNCATE %s", fqn)
    except Exception as e:  # มี FK อ้างถึง / ไม่มีสิทธิ์
        logger.warning("TRUNCATE ไม่ได้ (%s) -> ใช้ DELETE แทน", type(e).__name__)
        conn.execute(text(f"DELETE FROM {fqn};"))
        logger.info("🧹 DELETE %s", fqn)


def insert_rows(conn, df: pd.DataFrame, schema: str, table: str, chunksize: int, identity_cols: list[str]) -> None:
    has_identity = bool(identity_cols) and any(c in df.columns for c in identity_cols)
    fqn = f"[{schema}].[{table}]"

    if has_identity:
        logger.info("เปิด IDENTITY_INSERT (คอลัมน์ identity: %s)", identity_cols)
        conn.execute(text(f"SET IDENTITY_INSERT {fqn} ON;"))
    try:
        df.to_sql(
            name=table,
            con=conn,
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=chunksize,
        )
    finally:
        if has_identity:
            conn.execute(text(f"SET IDENTITY_INSERT {fqn} OFF;"))


# ------------------------------------------------------------------
def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--file", type=Path, default=DEFAULT_XLSX, help=f"ไฟล์ Excel (default: {DEFAULT_XLSX.name})")
    p.add_argument("--sheet", default=0, help="ชื่อหรือ index ของ sheet (default: 0)")
    p.add_argument("--env-file", type=Path, default=DEFAULT_ENV, help=f"ไฟล์ค่าเชื่อมต่อ (default: {DEFAULT_ENV.name})")
    p.add_argument("--dsn", default=None, help="SQLAlchemy URL เต็ม (ถ้าใส่จะไม่สนใจ host/user/password)")
    p.add_argument("--host", default=None)
    p.add_argument("--port", default=None)
    p.add_argument("--database", default=None)
    p.add_argument("--user", default=None)
    p.add_argument("--password", default=None)
    p.add_argument("--driver", choices=["pymssql", "pyodbc"], default=None, help="default: อันที่ติดตั้งอยู่")
    p.add_argument("--schema", default=DEFAULT_SCHEMA)
    p.add_argument("--table", default=DEFAULT_TABLE)
    p.add_argument("--chunksize", type=int, default=1000)
    p.add_argument("--apply", action="store_true", help="เขียน DB จริง (ถ้าไม่ใส่ = dry-run)")
    p.add_argument("--no-backup", action="store_true", help="ไม่ต้อง backup ตารางเดิม (ไม่แนะนำ)")
    p.add_argument("--allow-column-mismatch", action="store_true",
                   help="ยอมให้คอลัมน์ Excel กับ DB ไม่ตรงกัน (คอลัมน์ที่ขาดจะเป็น NULL)")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    if not args.file.exists():
        logger.error("ไม่พบไฟล์: %s", args.file)
        return 1

    # ---- อ่าน + ตรวจไฟล์ (ทำก่อนต่อ DB จะได้รู้ผลเร็ว) ----
    sheet = int(args.sheet) if str(args.sheet).isdigit() else args.sheet
    df = read_excel(args.file, sheet)
    validate_excel(df)

    # ---- ต่อ DB ----
    load_env_file(args.env_file)
    engine, _ = build_engine(args)

    fqn = f"[{args.schema}].[{args.table}]"
    with engine.connect() as conn:
        target_cols = get_target_columns(conn, args.schema, args.table)
        before = conn.execute(text(f"SELECT COUNT(*) FROM {fqn}")).scalar()

    identity_cols = [c["name"] for c in target_cols if c["identity"]]
    aligned, missing_in_excel, missing_in_db = align_columns(df, target_cols)
    aligned = to_db_values(aligned)

    logger.info("ตารางปลายทาง %s: %s แถว, %s คอลัมน์", fqn, before, len(target_cols))
    logger.info("จับคู่คอลัมน์ได้ %s / %s", len(aligned.columns), len(target_cols))
    if missing_in_excel:
        logger.warning("คอลัมน์ที่มีใน DB แต่ไม่มีใน Excel (จะเป็น NULL): %s", missing_in_excel)
    if missing_in_db:
        logger.warning("คอลัมน์ที่มีใน Excel แต่ไม่มีใน DB (จะถูกข้าม): %s", missing_in_db)
    if (missing_in_excel or missing_in_db) and not args.allow_column_mismatch:
        logger.error("คอลัมน์ไม่ตรงกัน — ตรวจสอบก่อน หรือใส่ --allow-column-mismatch ถ้าตั้งใจ")
        return 2

    logger.info("สรุป: %s แถวใน DB -> จะเหลือ %s แถว (%+d)", before, len(aligned), len(aligned) - before)

    if not args.apply:
        logger.info("")
        logger.info("*** DRY RUN — ยังไม่ได้เขียนอะไรลง DB ใส่ --apply เพื่อเขียนจริง ***")
        logger.info("ตัวอย่าง 3 แถวแรกที่จะเขียน:")
        logger.info("\n%s", aligned.head(3).to_string())
        return 0

    # ---- เขียนจริง ----
    with engine.begin() as conn:
        if not args.no_backup:
            backup_table(conn, args.schema, args.table)
        clear_table(conn, args.schema, args.table)
        insert_rows(conn, aligned, args.schema, args.table, args.chunksize, identity_cols)

    with engine.connect() as conn:
        after = conn.execute(text(f"SELECT COUNT(*) FROM {fqn}")).scalar()
        key = next((c["name"] for c in target_cols if normalize(c["name"]) == KEY_COL), KEY_COL)
        distinct = conn.execute(text(f"SELECT COUNT(DISTINCT [{key}]) FROM {fqn}")).scalar()

    logger.info("✅ เสร็จแล้ว: %s -> %s แถว | %s ไม่ซ้ำ %s ค่า", before, after, key, distinct)
    if after != len(aligned):
        logger.error("จำนวนแถวไม่ตรงกับที่ส่งไป (%s) — ตรวจสอบด่วน", len(aligned))
        return 3
    if distinct != after:
        logger.error("ยังมี %s ซ้ำในตารางปลายทาง — pipeline จะ fail ตอน extract", key)
        return 4
    return 0


if __name__ == "__main__":
    sys.exit(main())
