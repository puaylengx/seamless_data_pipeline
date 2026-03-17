# dags/src/education/student_information/transformers/student_information.py
from __future__ import annotations
import pandas as pd

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

def transform_student_information(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)

    # ✅ ทำให้มี key 2 ชื่อแบบชัดเจน (ไม่ rename ทิ้ง)
    if "student_id" not in df.columns and "code" in df.columns:
        df["student_id"] = df["code"]

    # กันเคสไม่มีคอลัมน์
    if "student_id" not in df.columns:
        df["student_id"] = pd.NA

    df = cast_and_clean(df)
    return df