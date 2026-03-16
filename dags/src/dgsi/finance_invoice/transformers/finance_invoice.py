import re
import pandas as pd
from datetime import datetime

TARGET_COLS_ORDER = [
    "invoiceId", "acaYear", "semester", "invoiceNo", "regisType",
    "invoiceAmount", "paidDate", "paidAmount", "paidStatus",
    "invoiceType", "schNameTh", "remark", "studentCode"
]

def clean_invoice_id(v):
    if v is None:
        return None
    s = str(v).strip()
    if s.isdigit():
        return s
    s2 = re.sub(r"\s|\u200b", "", s)
    return s2 if s2.isdigit() else None

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()

    df["invoiceId"] = df["invoiceId"].apply(clean_invoice_id)
    df = df.loc[df["invoiceId"].notna()].copy()

    df["paidDate"] = pd.to_datetime(df["paidDate"], errors="coerce")
    df = df.replace({pd.NaT: None, "None": None, "nan": None})

    df = df[TARGET_COLS_ORDER]

    # handle dup key: keep last (เหมือนเดิม)
    if df.duplicated(subset=["invoiceId"]).any():
        df = (
            df.sort_values(by=["studentCode", "acaYear", "semester", "invoiceId"])
              .drop_duplicates(subset=["invoiceId"], keep="last")
        )

    df = df.sort_values(by=["studentCode", "acaYear", "semester", "invoiceId"]).reset_index(drop=True)
    df["_row_order"] = range(1, len(df) + 1)
    return df