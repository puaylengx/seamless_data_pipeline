# dags/src/education/student_information/validators/student_information.py
from __future__ import annotations
import logging
import pandas as pd

logger = logging.getLogger("airflow.task")

# คอลัมน์ที่ถ้าว่างยกแผงแปลว่า join กับ dbo.StagingStudent ไม่ติด
# (เคยเกิดจริง: รอบ 05:00 ดึงข้อมูลตอน StagingStudent ยังว่าง แล้วเอา NULL ไปทับของดีใน BQ)
CRITICAL_NULL_RATIO = {
    "major_code": 0.30,
    "major_name": 0.30,
}


def validate_student_information(
    df: pd.DataFrame,
    min_rows: int = 1,
    critical_null_ratio: dict[str, float] | None = None,
) -> None:
    """
    ถ้าไม่ผ่านให้ raise Exception เพื่อให้ Task fail ทันที

    min_rows              จำนวนแถวขั้นต่ำที่ยอมรับได้ (กันเคสดึงมาไม่ครบ)
    critical_null_ratio   {column: สัดส่วน null สูงสุดที่ยอมรับได้} 0.30 = ห้ามเกิน 30%
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
    if len(df) < min_rows:
        raise ValueError(
            f"Validation failed: got {len(df)} rows, expected at least {min_rows}. "
            "อาจดึงข้อมูลตอนตารางต้นทางยังไม่พร้อม"
        )

    # ✅ กัน NULL ยกแผงไปเขียนทับข้อมูลดี
    ratios = CRITICAL_NULL_RATIO if critical_null_ratio is None else critical_null_ratio
    total = len(df)
    for col, max_ratio in ratios.items():
        if col not in df.columns:
            continue
        n_null = int(df[col].isna().sum())
        ratio = n_null / total
        logger.info("null check %s: %s/%s (%.1f%%)", col, n_null, total, ratio * 100)
        if ratio > max_ratio:
            raise ValueError(
                f"Validation failed: '{col}' is null in {n_null}/{total} rows "
                f"({ratio:.1%} > {max_ratio:.0%}). "
                "น่าจะ join dbo.StagingStudent ไม่ติด — ยกเลิกการโหลดเพื่อไม่ให้ทับข้อมูลเดิม"
            )

    # student_id ซ้ำจะทำให้ BigQuery MERGE พัง
    dup = int(df["student_id"].duplicated().sum())
    if dup:
        raise ValueError(
            f"Validation failed: {dup} duplicate student_id detected. "
            "MERGE ต้องการ 1 แถวต่อ 1 student_id"
        )
