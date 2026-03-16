import pandas as pd

REQUIRED_COLS = {
    "invoiceId", "acaYear", "semester", "invoiceNo", "regisType",
    "invoiceAmount", "paidDate", "paidAmount", "paidStatus",
    "invoiceType", "schNameTh", "remark", "studentCode"
}

def validate(df: pd.DataFrame) -> dict:
    issues = []

    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        issues.append(f"missing_columns={sorted(missing)}")

    if df.empty:
        issues.append("dataframe_empty")

    # invoiceId ต้องไม่ว่างและควร unique
    if "invoiceId" in df.columns:
        if df["invoiceId"].isna().any():
            issues.append("invoiceId_has_null")
        dup = int(df.duplicated(subset=["invoiceId"]).sum())
        if dup:
            issues.append(f"invoiceId_duplicates={dup}")

    # invoiceAmount > 0 (ตาม query เดิม)
    if "invoiceAmount" in df.columns:
        bad_amt = int((df["invoiceAmount"].fillna(0) <= 0).sum())
        if bad_amt:
            issues.append(f"invoiceAmount_non_positive={bad_amt}")

    return {
        "ok": len(issues) == 0,
        "issues": issues,
        "rows": int(len(df))
    }