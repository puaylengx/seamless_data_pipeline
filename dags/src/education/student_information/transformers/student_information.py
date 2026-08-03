# dags/src/education/student_information/transformers/student_information.py
from __future__ import annotations
import logging
import pandas as pd

logger = logging.getLogger("airflow.task")

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
        .str.lower()
    )
    return df

def cast_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    date_cols = [c for c in df.columns if c.endswith("_date")]
    for c in date_cols:
        df[c] = pd.to_datetime(df[c], errors="coerce")

    if "grade" in df.columns:
        df["grade"] = df["grade"].fillna("N/A")

    df = df.drop_duplicates()
    return df

def dedupe_by_student(df: pd.DataFrame) -> pd.DataFrame:
    """
    บีบให้เหลือ 1 แถวต่อ 1 student_id ให้ตรงกับ grain ของตารางปลายทาง

    dbo.StagingStudent มี studentCode ซ้ำได้ (เช่น 5580005) ทำให้ query คืนหลายแถวต่อคน
    ถ้าปล่อยไว้ BigQuery MERGE จะ error: UPDATE/MERGE must match at most one source row
    เก็บแถวที่ academic_year / academic_term ใหม่สุด
    """
    if "student_id" not in df.columns:
        return df

    dup = int(df["student_id"].duplicated().sum())
    if not dup:
        return df

    sort_cols = [c for c in ("academic_year", "academic_term") if c in df.columns]
    logger.warning(
        "⚠️ Found %s duplicate student_id, keeping latest by %s. Sample: %s",
        dup,
        sort_cols or "row order",
        df.loc[df["student_id"].duplicated(keep=False), "student_id"].unique()[:5].tolist(),
    )

    if sort_cols:
        df = df.sort_values(sort_cols, ascending=False, kind="stable")
    return df.drop_duplicates(subset=["student_id"], keep="first")


def transform_student_information(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)

    # ✅ ทำให้มี key 2 ชื่อแบบชัดเจน (ไม่ rename ทิ้ง)
    if "student_id" not in df.columns and "code" in df.columns:
        df["student_id"] = df["code"]

    # กันเคสไม่มีคอลัมน์
    if "student_id" not in df.columns:
        df["student_id"] = pd.NA

    df = cast_and_clean(df)
    df = dedupe_by_student(df)
    return df