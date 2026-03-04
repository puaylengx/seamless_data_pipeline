from __future__ import annotations
import pandas as pd

class ValidationError(Exception): 
    """Raised when the dataframe fails validation."""
    
def validate_student_df(df: pd.DataFrame) -> None:
    """Raise ValidationError if the frame is not suitable for loading."""
    # requirement column
    required: {
        "code", #primary key
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
        "ingestion_date",
        "created_at",
        "updated_at",
    }
    
    missing = required - set(df.columns)
    if missing:
        raise ValidationError(f"Missing columns: {missing}")
    
    if df["code"].isna.any() or (df["code"].astype(str).str.strip() == "").any():
        raise ValidationError("code must be non-empty")
    
    # กำหนด academic calendar ในช่วงที่เหมาะสม
    if "academic_year" in df and (df["academic_year"].dropna() < 1900).any():
        raise ValidationError("academic_year has invalid values")
    
    # created_at / updated_at ต้องแปลงเวลาได้ (จะเป็น dtype datetime64[ns] หลัง normalize)
    for col in ("created_at", "updated_at"):
        if not pd.api.types.is_datetime64_ns_dtype(df[col].dtype):
            raise ValidationError(f"{col} must be datetime64[ns] (use normalize step)")
