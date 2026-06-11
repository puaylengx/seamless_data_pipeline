from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger("airflow.task")

TALENT_COLS = [
    "talent_foreign_language",
    "talent_computer",
    "talent_visual_art",
    "talent_performing_arts",
    "talent_sports",
    "talent_academic",
    "talent_other",
]

# excel column (after normalize) -> target df column (after normalize_columns)
EXTRA_COLS: dict[str, str] = {
    "number_of_siblings":                "numberofsiblings",
    "number_of_siblings_still_studying": "numberofsiblingsstillstudying",
    "you_are_child_number":              "sequencechild",
}


def load_excel_data(file_path: str | Path, sheet: int | str = 0) -> pd.DataFrame:
    """
    Read talent + extra columns from Excel.
    Returns a DataFrame with student_id + talent_* + extra columns.
    All columns are lowercased and space-replaced with underscore.
    """
    df = pd.read_excel(file_path, sheet_name=sheet, dtype=str)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    missing = [c for c in TALENT_COLS if c not in df.columns]
    if missing:
        logger.warning("Excel: talent columns not found: %s", missing)

    keep = (
        ["student_id"]
        + [c for c in TALENT_COLS if c in df.columns]
        + [c for c in EXTRA_COLS if c in df.columns]
    )
    df = df[[c for c in keep if c in df.columns]].copy()

    def _norm_id(val: str) -> str:
        v = str(val).strip()
        return v[:-2] if v.endswith(".0") else v

    df["student_id"] = df["student_id"].apply(_norm_id)
    df = df[df["student_id"].notna() & (df["student_id"] != "nan")].reset_index(drop=True)

    logger.info("Excel: %d rows loaded (file: %s)", len(df), file_path)
    return df
