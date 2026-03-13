# dags/src/education/student_information/validators/student_information.py
from __future__ import annotations
import pandas as pd

def validate_student_information(df: pd.DataFrame) -> None:
    """
    ถ้าไม่ผ่านให้ raise Exception เพื่อให้ Task fail ทันที
    """
    if df.empty:
        raise ValueError("Validation failed: DataFrame is empty.")

    required = ["student_id", "first_name_en", "last_name_en"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Validation failed: Missing required columns: {missing_cols}")

    # ตัวอย่าง not-null check
    if df["student_id"].isna().any():
        raise ValueError("Validation failed: Null student_id detected.")

    # จำนวนอย่างน้อย
    if len(df) < 1:
        raise ValueError("Validation failed: No records.")
