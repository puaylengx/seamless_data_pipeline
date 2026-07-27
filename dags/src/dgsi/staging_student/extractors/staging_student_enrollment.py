from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import text

from src.dgsi.staging_student.extractors.staging_student_sql_server import (
    build_engine_from_airflow_conn,
)

logger = logging.getLogger("airflow.task")

TALENT_COLS = [
    "talent_foreign_language",
    "talent_computer",
    "talent_visual_art",
    "talent_performing_arts",
    "talent_sports",
    "talent_academic",
    "talent_other",
]

# enrollment column (after normalize) -> target df column (after normalize_columns)
EXTRA_COLS: dict[str, str] = {
    "number_of_siblings":                "numberofsiblings",
    "number_of_siblings_still_studying": "numberofsiblingsstillstudying",
    "you_are_child_number":              "sequencechild",
}

# enrollment column -> target df column ที่ staging_student.sql ปล่อยเป็น NULL ไว้
# คอลัมน์กลุ่มนี้มีช่อง free-text คู่กัน (<col>_option) ที่ผู้กรอกใช้เมื่อเลือก "Other"
FILL_COLS: dict[str, str] = {
    "religion":       "religionname",
    "race":           "racename",
    "marital_status": "maritalstatusname",
}

OPTION_SUFFIX = "_option"

KEY_COL = "student_id"

ENROLLMENT_SQL = "SELECT * FROM dbo.[candidate-62_66]"


def wanted_columns() -> list[str]:
    """
    คอลัมน์ (หลัง normalize) ที่ pipeline ใช้จริงจาก muic_enrollment
    ใช้กรองทิ้งคอลัมน์ที่เหลือ (father_*, mother_*, address ฯลฯ) ไม่ให้หลุดไปถึง target table
    """
    return (
        [KEY_COL]
        + TALENT_COLS
        + list(EXTRA_COLS)
        + [c for base in FILL_COLS for c in (base, base + OPTION_SUFFIX)]
    )


def check_duplicate_student_id(df: pd.DataFrame) -> None:
    """
    ตรวจ student_id ซ้ำ — ถ้าเจอให้ fail task พร้อมบอกว่าใครซ้ำและฟิลด์ไหนต่างกัน

    ทำไมต้อง fail แทนที่จะเลือกใบใดใบหนึ่งเอง:
    ใบซ้ำที่ค่าไม่ตรงกันจะทำให้ merge ได้ผลไม่คงที่ (ขึ้นกับลำดับแถวที่ SQL Server คืนมา
    เพราะ SELECT ไม่มี ORDER BY) → ข้อมูลใน target สลับไปมาทุกรอบ + เกิด UPDATE ปลอมใน audit
    """
    dup = df[df[KEY_COL].duplicated(keep=False)]
    if dup.empty:
        logger.info("Enrollment DB: ไม่มี %s ซ้ำ ✅", KEY_COL)
        return

    fields = [c for c in df.columns if c != KEY_COL]
    lines = []
    for sid, g in dup.groupby(KEY_COL):
        diff = [c for c in fields if g[c].fillna("~NULL~").nunique() > 1]
        lines.append(
            f"  - {sid}: {len(g)} ใบ | "
            + (f"ฟิลด์ที่ต่าง ({len(diff)}): {', '.join(diff)}" if diff else "เหมือนกันทุกฟิลด์")
        )

    detail = "\n".join(lines)
    logger.error(
        "พบ %s ซ้ำ %d คน (%d แถว) ในตาราง enrollment:\n%s",
        KEY_COL, dup[KEY_COL].nunique(), len(dup), detail,
    )
    raise ValueError(
        f"พบ {KEY_COL} ซ้ำ {dup[KEY_COL].nunique()} คน ({len(dup)} แถว) ในตาราง enrollment "
        f"— แก้ข้อมูลต้นทางก่อน หรือกำหนดกฎเลือกใบให้ชัดเจน\n{detail}"
    )


def extract_enrollment_data(enrollment_conn) -> pd.DataFrame:
    """
    ดึงข้อมูล talent + extra + fill columns จาก muic_enrollment.dbo.[candidate-62_66]
    คืน DataFrame ที่ normalize column names แล้ว (lowercase + underscore)
    และเหลือเฉพาะคอลัมน์ใน wanted_columns()
    """
    engine = build_engine_from_airflow_conn(enrollment_conn)

    with engine.connect() as conn:
        df = pd.read_sql(text(ENROLLMENT_SQL), conn)

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    if KEY_COL not in df.columns:
        raise ValueError(
            f"Column '{KEY_COL}' not found in enrollment table. "
            f"Available columns: {list(df.columns)}"
        )

    wanted = wanted_columns()
    missing = [c for c in wanted if c not in df.columns]
    if missing:
        logger.warning("Enrollment DB: columns not found: %s", missing)

    df = df[[c for c in wanted if c in df.columns]].copy()

    def _norm_id(val) -> str:
        v = str(val).strip()
        return v[:-2] if v.endswith(".0") else v

    df[KEY_COL] = df[KEY_COL].apply(_norm_id)
    df = df[df[KEY_COL].notna() & (df[KEY_COL] != "nan")].reset_index(drop=True)

    check_duplicate_student_id(df)

    logger.info(
        "Enrollment DB: %d rows loaded, %d columns kept", len(df), len(df.columns)
    )
    return df
