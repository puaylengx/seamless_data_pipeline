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
    # ตัวอย่างการแปลง (ปรับให้ตรง schema จริงของคุณ)
    date_cols = [c for c in df.columns if c.endswith("_date")]
    for c in date_cols:
        df[c] = pd.to_datetime(df[c], errors="coerce")

    # เติมค่า missing ที่จำเป็น
    if "grade" in df.columns:
        df["grade"] = df["grade"].fillna("N/A")

    # ตัดแถวซ้ำ
    df = df.drop_duplicates()

    return df

def transform_student_information(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)
    rename_map = {
            "code": "student_id",
    }

    df = df.rename(columns=rename_map)

    # กันเคสไม่มีคอลัมน์ที่ต้องใช้
    for col in ["student_id"]:
        if col not in df.columns:
            df[col] = pd.NA

    df = cast_and_clean(df)
    return df