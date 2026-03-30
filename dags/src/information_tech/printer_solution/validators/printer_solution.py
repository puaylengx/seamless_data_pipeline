# validators/printer_solution.py

import pandas as pd

def validate_printer_solution(df: pd.DataFrame) -> None:
    if df.empty:
        raise Exception("Validation failed : Printer solution dataframe is empty")

    required = ["user_name",
        "full_name",
        "department",
        "office",
        "job_type",
        "total_color_pages",
        "total_grayscale_pages",
        "total_pages",
        "total_cost_color_pages",
        "total_cost_grayscale_pages",
        "total_cost_pages",
        "usage_calendar_year",
        "usage_budget_year",
        "usage_calendar_month",
        "usage_budget_month_order",
    ]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Validation failed: Missing required columns: {missing_cols}")

    if df["user_name"].isna().any():
        raise ValueError("Validation failed: Null user_name detected.")

    # จำนวนอย่างน้อย
    if len(df) < 1:
        raise ValueError("Validation failed: No records.")