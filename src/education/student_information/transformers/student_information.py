from __future__ import annotations
from typing import Iterable
import pandas as pd

CANONICAL_COLUMNS = [
    "code",
    "title",
    "first_name_en",
    "middle_name_en",
    "last_name_en",
    "gender",
    "nationality",
    "resident_type",
    "student_fee_type",
    "academic_year",
    "academic_term",
    "admission_type",
    "student_type",
    "student_status",
    "major_code",
    "major_name",
    "division",
    "division_name",
    "is_active",
    "ingestion_date",   # ต้องการเป็น DATE
    "created_at",       # TIMESTAMP (UTC, tz-naive)
    "updated_at",       # TIMESTAMP (UTC, tz-naive)
]

STRING_COLUMNS: Iterable[str] = [
    "code",
    "title",
    "first_name_en",
    "middle_name_en",
    "last_name_en",
    "gender",
    "nationality",
    "resident_type",
    "student_fee_type",
    "academic_term",
    "admission_type",
    "student_type",
    "student_status",
    "major_code",
    "major_name",
    "division",
    "division_name",
]


def normalize_student_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Apply renaming, trimming and timestamp normalization."""
    if raw_df.empty:
        return raw_df.copy()
    
    df = raw_df.copy()
    
    # set ingestion_date to date
    ingestion_ts = pd.Timestamp.utcnow()
    df["ingestion_date"] = ingestion_ts.date() # <- load เป็น BigQuery DATE
    
    # สร้าง created_at / updated_at ถ้าไม่มี (ใช้เวลาเดียวกัน)
    for column in ("created_at","updated_at"):
        if column in df:
            df[column] = df[column].fillna("").astype(str).str.strip()
        else:
            df[column] = ""
            
    # clean string column
    df["code"] = df["code"].str.upper()
    if "is_active" in df:
        df["is_active"] = df["is_active"].fillna(True).astype(bool)
    else:
        df["is_active"] = True
        
    # set time column to UTC tz-native (เหมาะกับ BQ TIMESTAMP)
    for column in ("created_at","updated_at"):
        df[column] = pd.to_datetime(df[column], errors="coerce", utc=True).dt.tz_localize(None)
    
    # academic year
    if "academic_year" in df:
        df["academic_year"] = pd.to_numeric(df["academic_year"], errors="coerce").astype("Int64")
        
    # add column ให้ครบ
    missing_columns = [col for col in CANONICAL_COLUMNS if col not in df]
    for column in missing_columns:
        df[column] = pd.NA
        
    return df[CANONICAL_COLUMNS]
      
    
    # set column name to lower case
    # df.columns = [c.strip().lower() for c in df.columns]
    
    # edit column name, data type, trim, uppercase etc.
    # df = df.rename(columns={"ID": "student_id"})
    # df["student_id"] = df ["student_id"].astype(str).str.strip()
    # return df