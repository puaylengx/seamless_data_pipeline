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


STATUS_COL = "studentstatusname"


def _log_status_trace(df: pd.DataFrame, stage: str) -> None:
    """
    log สถานะคอลัมน์ studentstatusname ที่แต่ละ step:
    - total = จำนวนแถว
    - filled = แถวที่ studentstatusname ไม่ว่าง
    - empty  = แถวที่ว่าง/หาย (ควรเป็น 0 ตลอดเส้น)
    """
    if STATUS_COL not in df.columns:
        logger.warning("STATUS_TRACE [%-18s] col '%s' MISSING (rows=%d)", stage, STATUS_COL, len(df))
        return

    s = df[STATUS_COL]
    total = len(df)
    filled = int((s.notna() & (s.astype(str).str.strip() != "")).sum())
    empty = total - filled
    logger.info(
        "STATUS_TRACE [%-18s] rows=%d filled=%d empty=%d", stage, total, filled, empty
    )
    if empty:
        sample = (
            df.loc[
                s.isna() | (s.astype(str).str.strip() == ""),
                KEY_COL if KEY_COL in df.columns else df.columns[0],
            ]
            .astype(str)
            .head(10)
            .tolist()
        )
        logger.warning("STATUS_TRACE [%-18s] empty sample keys=%s", stage, sample)


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
    เติม studentstatus ที่เป็น null ด้วย studentstatusname (เฉพาะเมื่อมีคอลัมน์ studentstatus ดิบ)
    หมายเหตุ: studentstatusname ต้องคง flow ไปถึง loader เสมอ (มาจาก query โดยตรง)
    → drop เฉพาะกรณีที่มีคอลัมน์ studentstatus ดิบมารองรับจริง ไม่ใช่ลบทิ้งทุกครั้ง
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

        # มี studentstatus ดิบอยู่แล้ว → drop studentstatusname ได้
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

            # vectorized: มีอักษรไทยจริง → ใช้ค่าไทย, ไม่งั้น fallback เป็น EN
            th_str = df[th_col].where(df[th_col].notna(), "").astype(str)
            has_thai = th_str.str.contains(THAI_REGEX.pattern, regex=True)
            df[th_col] = df[th_col].where(has_thai, df[en_col])

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
    df_enrollment: Optional[pd.DataFrame] = None,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    ✅ Transform สำหรับ Airflow (เหมือน Finance Invoice)
    - normalize column names (lowercase)
    - merge enrollment DB data (talent + extra columns) ถ้าส่ง df_enrollment มา
    - clean/drop bad keys
    - clean emails
    - fill studentstatus from studentstatusname (เฉพาะเมื่อมี studentstatus ดิบ); คง studentstatusname ไว้เสมอ
    - fallback Thai names
    - drop duplicates by key
    - return (df_clean, metrics)

    metrics = {"fixed": int, "dropped": int}
    """
    audit_writer = audit_writer or _noop_audit
    metrics = {"fixed": 0, "dropped": 0}

    # normalize
    df = normalize_columns(df)
    _log_status_trace(df, "after_normalize")

    # ถ้าไม่มี key ให้คืนไป (ให้ loader ตัดสิน)
    if KEY_COL not in df.columns:
        return df.reset_index(drop=True), metrics

    # merge enrollment DB data (talent + extra columns)
    if df_enrollment is not None and not df_enrollment.empty:
        df = merge_enrollment_data(df, df_enrollment)
        _log_status_trace(df, "after_enrollment")
    else:
        logger.warning("df_enrollment not provided, skipping enrollment merge")

    # drop bad keys
    df, dropped = drop_bad_keys(df, audit_writer)
    metrics["dropped"] += dropped
    _log_status_trace(df, "after_drop_keys")

    # fix emails
    df, fixed = fix_emails(df, audit_writer)
    metrics["fixed"] += fixed

    # fill status + drop statusname
    df, fixed = fill_studentstatus(df, audit_writer)
    metrics["fixed"] += fixed
    _log_status_trace(df, "after_fill_status")

    # fallback Thai names
    df, fixed = fallback_thai_names(df, audit_writer)
    metrics["fixed"] += fixed

    # drop duplicates
    df, dropped = drop_duplicates_by_key(df)
    metrics["dropped"] += dropped
    _log_status_trace(df, "final")

    return df.reset_index(drop=True), metrics


# ============================================================
# 5) Enrollment merge helpers
# ============================================================

# prefix ชั่วคราวของคอลัมน์ฝั่ง enrollment ระหว่าง merge
# กันชื่อชนกับคอลัมน์ฝั่ง SQL (เช่น email) และทำให้ drop ทิ้งทีเดียวได้ครบ
ENROLLMENT_PREFIX = "_enr_"


def _clean_item(s: str) -> str:
    s = s.strip()
    if s.startswith("- "):
        s = s[2:].strip()
    return "" if s in ("-", "") else s.strip()


def _build_talent_series(df: pd.DataFrame, talent_cols: list[str]) -> pd.Series:
    """
    รวมทุกคอลัมน์ talent_* เป็นสตริงเดียว คั่นด้วย ", "
    - แต่ละคอลัมน์อาจมีหลายรายการคั่นด้วย newline
    - ตัด bullet "- " และรายการว่าง/"-" ทิ้ง
    """
    cols = [c for c in talent_cols if c in df.columns]
    if not cols:
        return pd.Series([None] * len(df), index=df.index, dtype="object")

    sub = df[cols].where(df[cols].notna(), "").astype(str)

    # ต่อทุกคอลัมน์เข้าด้วยกันก่อน (vectorized) แล้วค่อย clean ทีเดียวต่อแถว
    joined = sub[cols[0]]
    for c in cols[1:]:
        joined = joined.str.cat(sub[c], sep="\n")

    def _clean_row(raw: str) -> str | None:
        parts = [
            cleaned
            for item in raw.replace("\r\n", "\n").split("\n")
            if (cleaned := _clean_item(item))
        ]
        result = ", ".join(parts)
        return result[:255] if result else None

    return joined.map(_clean_row)


def _is_blank(s: pd.Series) -> pd.Series:
    """แถวที่ถือว่า 'ไม่มีค่า' → NaN/None หรือสตริงว่าง"""
    return s.isna() | (s.astype(str).str.strip() == "")


def _fill_if_blank(df: pd.DataFrame, target_col: str, values: pd.Series) -> None:
    """
    เติมค่าจาก enrollment เฉพาะตอนที่ฝั่ง SQL ว่าง (SQL ชนะเมื่อมีค่าทั้งคู่)
    """
    if target_col not in df.columns:
        df[target_col] = values
        return
    df[target_col] = df[target_col].where(~_is_blank(df[target_col]), values)


def _resolve_option(df: pd.DataFrame, base_col: str, option_suffix: str) -> pd.Series:
    """
    ค่าหลักเป็น "Other" (หรือว่าง) และมีข้อความใน <base>_option → ใช้ค่าใน _option แทน
    """
    base = df[base_col] if base_col in df.columns else pd.Series(
        [None] * len(df), index=df.index, dtype="object"
    )

    option_col = base_col + option_suffix
    if option_col not in df.columns:
        return base

    option = df[option_col]
    use_option = ~_is_blank(option) & (
        _is_blank(base) | (base.astype(str).str.strip().str.lower() == "other")
    )
    return base.where(~use_option, option)


def merge_enrollment_data(
    df: pd.DataFrame,
    df_enrollment: pd.DataFrame,
) -> pd.DataFrame:
    """
    Enrich the main DataFrame (after normalize_columns) with enrollment DB data.
    Fills: talentname, numberofsiblings, numberofsiblingsstillstudying, sequencechild,
           religionname, racename, maritalstatusname.
    Keys: df["studentcode"] <-> df_enrollment["student_id"]

    หลักการ: ค่าจาก staging_student.sql ชนะเสมอ — enrollment เติมเฉพาะช่องที่ว่าง
    และคอลัมน์ฝั่ง enrollment ทั้งหมดถูก drop ทิ้งหลัง merge (ไม่ให้หลุดไป target table)
    """
    from src.dgsi.staging_student.extractors.staging_student_enrollment import (
        EXTRA_COLS,
        FILL_COLS,
        OPTION_SUFFIX,
        TALENT_COLS,
    )
    from src.dgsi.staging_student.extractors.staging_student_enrollment import (
        KEY_COL as ENR_KEY_COL,
    )

    df = df.copy()
    df[KEY_COL] = df[KEY_COL].astype(str).str.strip()

    sql_codes = set(df[KEY_COL])
    enrollment_codes = set(df_enrollment[ENR_KEY_COL])
    overlap = sql_codes & enrollment_codes

    logger.info(
        "Enrollment merge: %d SQL codes, %d enrollment codes, %d overlap",
        len(sql_codes), len(enrollment_codes), len(overlap),
    )
    if not overlap:
        logger.warning("No matching studentCode between SQL and enrollment DB. Sample SQL: %s", sorted(sql_codes)[:5])
        logger.warning("Sample enrollment: %s", sorted(enrollment_codes)[:5])

    p = ENROLLMENT_PREFIX
    right = df_enrollment.rename(columns={c: p + c for c in df_enrollment.columns})
    df = df.merge(right, left_on=KEY_COL, right_on=p + ENR_KEY_COL, how="left")

    _fill_if_blank(df, "talentname", _build_talent_series(df, [p + c for c in TALENT_COLS]))

    for enrollment_col, target_col in EXTRA_COLS.items():
        if p + enrollment_col in df.columns:
            _fill_if_blank(df, target_col, df[p + enrollment_col])

    for enrollment_col, target_col in FILL_COLS.items():
        _fill_if_blank(df, target_col, _resolve_option(df, p + enrollment_col, OPTION_SUFFIX))

    # drop ทุกคอลัมน์ที่มาจากฝั่ง enrollment ไม่ให้หลุดไปถึง target table
    df.drop(
        columns=[c for c in df.columns if c.startswith(p)],
        errors="ignore",
        inplace=True,
    )

    for col in ["talentname", *EXTRA_COLS.values(), *FILL_COLS.values()]:
        if col in df.columns:
            filled = int((~_is_blank(df[col])).sum())
            logger.info(
                "%s filled: %d / %d rows (%.1f%%)",
                col, filled, len(df), 100 * filled / len(df) if len(df) else 0,
            )

    return df