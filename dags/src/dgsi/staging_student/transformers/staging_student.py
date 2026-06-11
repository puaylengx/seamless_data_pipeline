from __future__ import annotations

import re
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Callable, Tuple, Dict, Any, Optional

logger = logging.getLogger("airflow.task")


# ============================================================
# 1) Constants / Regex
# ============================================================

THAI_REGEX = re.compile(r"[\u0E00-\u0E7F]")
EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

# ชื่อคอลัมน์มาตรฐานหลัง normalize (lowercase)
KEY_COL = "studentcode"


# ============================================================
# 2) Small helpers
# ============================================================

def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _noop_audit(_: Dict[str, Any]) -> None:
    return


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names → lowercase + underscore
    """
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
        .str.lower()
    )
    return df


def fallback_th(th_name, en_name):
    """
    เลือกชื่อภาษาไทย ถ้ามีอักษรไทยจริง ไม่เช่นนั้น fallback เป็น EN
    """
    if isinstance(th_name, str) and THAI_REGEX.search(th_name):
        return th_name
    return en_name


def clean_email(value: str | None) -> str | None:
    """
    - เลือกอีเมลตัวแรกถ้ามีหลายตัว (คั่นด้วย , ; หรือ space)
    - ตัดอักขระคร่อม
    - ไม่ผ่านรูปแบบ → คืน None
    """
    if value is None:
        return None

    s = str(value).strip()
    if not s or s.lower() in {"none", "null", "nan"}:
        return None

    for sep in [",", ";", " "]:
        if sep in s:
            s = s.split(sep)[0].strip()

    s = s.strip("<>\"'()[]")
    return s if EMAIL_REGEX.match(s) else None


def clean_student_code(value) -> str | None:
    """
    - ตัดช่องว่าง/อักขระแปลก
    - ต้องเป็นตัวเลขล้วน
    """
    if value is None:
        return None

    s = str(value).strip()
    if s.isdigit():
        return s

    s2 = re.sub(r"\s|\u200b", "", s)
    return s2 if s2.isdigit() else None


# ============================================================
# 3) Business rules helpers
# ============================================================

def drop_bad_keys(df: pd.DataFrame, audit_writer: Callable[[Dict[str, Any]], None]) -> Tuple[pd.DataFrame, int]:
    """
    clean key + drop rows ที่ key invalid
    return (df_clean, dropped_count)
    """
    df = df.copy()

    df[f"{KEY_COL}_raw"] = df[KEY_COL]
    df[KEY_COL] = df[KEY_COL].apply(clean_student_code)

    bad_key = df[KEY_COL].isna()
    dropped = int(bad_key.sum())

    if dropped:
        for sid in df.loc[bad_key, f"{KEY_COL}_raw"].tolist():
            audit_writer({
                "ts": _now_ts(),
                "action": "DROP_ROW_BAD_KEY",
                f"{KEY_COL}_raw": sid,
            })
        df = df.loc[~bad_key].copy()

    df.drop(columns=[f"{KEY_COL}_raw"], inplace=True, errors="ignore")
    return df, dropped


def fix_emails(df: pd.DataFrame, audit_writer: Callable[[Dict[str, Any]], None]) -> Tuple[pd.DataFrame, int]:
    """
    clean email แล้ว audit เฉพาะที่เปลี่ยนจริง
    return (df_clean, fixed_count)
    """
    df = df.copy()
    if "email" not in df.columns:
        return df, 0

    before = df["email"].copy()
    df["email"] = df["email"].apply(clean_email)

    changed = (before.fillna("") != df["email"].fillna(""))
    fixed = int(changed.sum())

    if fixed:
        for idx, row in df.loc[changed].iterrows():
            audit_writer({
                "ts": _now_ts(),
                "action": "FIX_EMAIL",
                KEY_COL: row.get(KEY_COL),
                "old": str(before.iloc[idx]),
                "new": str(row.get("email")),
            })

    return df, fixed


def fill_studentstatus(df: pd.DataFrame, audit_writer: Callable[[Dict[str, Any]], None]) -> Tuple[pd.DataFrame, int]:
    """
    ถ้ามีทั้ง studentstatus และ studentstatusname:
    - เติม studentstatus ที่เป็น null ด้วย studentstatusname
    - แล้ว drop studentstatusname ทิ้ง (เหมือน main.py เดิม)
    return (df_clean, fixed_count)
    """
    df = df.copy()
    fixed = 0

    if "studentstatus" in df.columns and "studentstatusname" in df.columns:
        missing = df["studentstatus"].isna()
        fixed = int(missing.sum())
        if fixed:
            df.loc[missing, "studentstatus"] = df.loc[missing, "studentstatusname"]
            for sid in df.loc[missing, KEY_COL].tolist():
                audit_writer({
                    "ts": _now_ts(),
                    "action": "FILL_STATUS_FROM_STATUSNAME",
                    KEY_COL: sid,
                })

    # drop ทิ้งเสมอถ้ามี (กัน MERGE อ้าง column ที่ไม่มีใน target)
    if "studentstatusname" in df.columns:
        df = df.drop(columns=["studentstatusname"])

    return df, fixed


def fallback_thai_names(df: pd.DataFrame, audit_writer: Callable[[Dict[str, Any]], None]) -> Tuple[pd.DataFrame, int]:
    """
    fallback ชื่อไทยจาก EN เฉพาะคอลัมน์ที่มีจริง
    return (df_clean, fixed_count)
    """
    df = df.copy()
    fixed = 0

    name_pairs = [
        ("firstnameth", "firstnameen"),
        ("middlenameth", "middlenameen"),
        ("lastnameth", "lastnameen"),
    ]

    for th_col, en_col in name_pairs:
        if th_col in df.columns and en_col in df.columns:
            before = df[th_col].astype(str)
            df[th_col] = df.apply(lambda r: fallback_th(r[th_col], r[en_col]), axis=1)

            changed = (before.fillna("") != df[th_col].astype(str).fillna(""))
            c = int(changed.sum())
            if c:
                fixed += c
                for idx, row in df.loc[changed].iterrows():
                    audit_writer({
                        "ts": _now_ts(),
                        "action": "FALLBACK_THAI_NAME",
                        KEY_COL: row.get(KEY_COL),
                        "field": th_col,
                        "old": str(before.iloc[idx]),
                        "new": str(row.get(th_col)),
                    })

    return df, fixed


def drop_duplicates_by_key(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    drop duplicate by key_col keep last
    return (df_clean, dropped_count)
    """
    df = df.copy()
    if KEY_COL not in df.columns:
        return df, 0

    before = len(df)
    df = df.drop_duplicates(subset=[KEY_COL], keep="last")
    dropped = before - len(df)
    return df, int(dropped)


# ============================================================
# 4) Main transform (Airflow-friendly, finance-invoice style)
# ============================================================

def transform_staging_student(
    df: pd.DataFrame,
    audit_writer: Optional[Callable[[Dict[str, Any]], None]] = None,
    excel_path: Optional[str] = None,
    excel_sheet: int | str = 0,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    ✅ Transform สำหรับ Airflow (เหมือน Finance Invoice)
    - normalize column names (lowercase)
    - merge Excel talent/extra data (ถ้าระบุ excel_path)
    - clean/drop bad keys
    - clean emails
    - fill studentstatus from studentstatusname แล้ว drop studentstatusname
    - fallback Thai names
    - drop duplicates by key
    - return (df_clean, metrics)

    metrics = {"fixed": int, "dropped": int}
    """
    audit_writer = audit_writer or _noop_audit
    metrics = {"fixed": 0, "dropped": 0}

    # normalize
    df = normalize_columns(df)

    # ถ้าไม่มี key ให้คืนไป (ให้ loader ตัดสิน)
    if KEY_COL not in df.columns:
        return df.reset_index(drop=True), metrics

    # merge Excel (talent + extra columns)
    if excel_path and Path(excel_path).exists():
        df = merge_excel_data(df, excel_path, excel_sheet)
    elif excel_path:
        logger.warning("Excel file not found, skipping merge: %s", excel_path)

    # drop bad keys
    df, dropped = drop_bad_keys(df, audit_writer)
    metrics["dropped"] += dropped

    # fix emails
    df, fixed = fix_emails(df, audit_writer)
    metrics["fixed"] += fixed

    # fill status + drop statusname
    df, fixed = fill_studentstatus(df, audit_writer)
    metrics["fixed"] += fixed

    # fallback Thai names
    df, fixed = fallback_thai_names(df, audit_writer)
    metrics["fixed"] += fixed

    # drop duplicates
    df, dropped = drop_duplicates_by_key(df)
    metrics["dropped"] += dropped

    return df.reset_index(drop=True), metrics


# ============================================================
# 5) Excel merge helpers
# ============================================================

def _clean_item(s: str) -> str:
    s = s.strip()
    if s.startswith("- "):
        s = s[2:].strip()
    return "" if s in ("-", "") else s.strip()


def _build_talent_name(row: pd.Series) -> str | None:
    from src.dgsi.staging_student.extractors.staging_student_excel import TALENT_COLS
    parts = []
    for col in TALENT_COLS:
        val = row.get(col)
        if pd.notna(val) and str(val).strip():
            for item in str(val).replace("\r\n", "\n").split("\n"):
                cleaned = _clean_item(item)
                if cleaned:
                    parts.append(cleaned)
    result = ", ".join(parts)
    return result[:255] if result else None


def merge_excel_data(
    df: pd.DataFrame,
    excel_path: str | Path,
    sheet: int | str = 0,
) -> pd.DataFrame:
    """
    Enrich the main DataFrame (after normalize_columns) with Excel data.
    Fills: talentname, numberofsiblings, numberofsiblingsstillstudying, sequencechild.
    Keys: df["studentcode"] <-> excel["student_id"]
    """
    from src.dgsi.staging_student.extractors.staging_student_excel import (
        EXTRA_COLS,
        TALENT_COLS,
        load_excel_data,
    )

    df_excel = load_excel_data(excel_path, sheet)
    df = df.copy()
    df[KEY_COL] = df[KEY_COL].astype(str).str.strip()

    sql_codes = set(df[KEY_COL])
    excel_codes = set(df_excel["student_id"])
    overlap = sql_codes & excel_codes

    logger.info(
        "Excel merge: %d SQL codes, %d Excel codes, %d overlap",
        len(sql_codes), len(excel_codes), len(overlap),
    )
    if not overlap:
        logger.warning("No matching studentCode between SQL and Excel. Sample SQL: %s", sorted(sql_codes)[:5])
        logger.warning("Sample Excel: %s", sorted(excel_codes)[:5])

    df = df.merge(df_excel, left_on=KEY_COL, right_on="student_id", how="left")

    df["talentname"] = df.apply(_build_talent_name, axis=1)

    for excel_col, target_col in EXTRA_COLS.items():
        if excel_col in df.columns:
            if target_col in df.columns:
                df[target_col] = df[excel_col].where(df[excel_col].notna(), df[target_col])
            else:
                df[target_col] = df[excel_col]

    drop_cols = (
        ["student_id"]
        + [c for c in TALENT_COLS if c in df.columns]
        + [c for c in EXTRA_COLS if c in df.columns]
    )
    df.drop(columns=drop_cols, errors="ignore", inplace=True)

    filled = df["talentname"].notna().sum()
    logger.info(
        "talentName filled: %d / %d rows (%.1f%%)",
        filled, len(df), 100 * filled / len(df) if len(df) else 0,
    )
    return df