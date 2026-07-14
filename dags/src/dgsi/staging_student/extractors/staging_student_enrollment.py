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

ENROLLMENT_SQL = "SELECT * FROM dbo.[candidate-62_66]"


def extract_enrollment_data(enrollment_conn) -> pd.DataFrame:
    """
    ดึงข้อมูล talent + extra columns จาก muic_enrollment.dbo.[candidate-62_66]
    คืน DataFrame ที่ normalize column names แล้ว (lowercase + underscore)
    """
    engine = build_engine_from_airflow_conn(enrollment_conn)

    with engine.connect() as conn:
        df = pd.read_sql(text(ENROLLMENT_SQL), conn)

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    if "student_id" not in df.columns:
        raise ValueError(
            f"Column 'student_id' not found in enrollment table. "
            f"Available columns: {list(df.columns)}"
        )

    def _norm_id(val) -> str:
        v = str(val).strip()
        return v[:-2] if v.endswith(".0") else v

    df["student_id"] = df["student_id"].apply(_norm_id)
    df = df[df["student_id"].notna() & (df["student_id"] != "nan")].reset_index(drop=True)

    missing_talent = [c for c in TALENT_COLS if c not in df.columns]
    if missing_talent:
        logger.warning("Enrollment DB: talent columns not found: %s", missing_talent)

    logger.info("Enrollment DB: %d rows loaded", len(df))
    return df
