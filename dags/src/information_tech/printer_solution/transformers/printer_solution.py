# transforms/printer_solution.py

import pandas as pd

def normalize_data_type(df: pd.DataFrame) -> pd.DataFrame:
    """Cast data types for BigQuery loading."""
    df = df.copy()
    df = df.astype(
        {
            "user_name": "string",
            "full_name": "string",
            "department": "string",
            "office": "string",
            "job_type": "string",
            "total_color_pages": "Int64",
            "total_grayscale_pages": "Int64",
            "total_pages": "Int64",
            "total_cost_color_pages": "float64",
            "total_cost_grayscale_pages": "float64",
            "total_cost_pages": "float64",
            "usage_calendar_year": "Int64",
            "usage_budget_year": "Int64",
            "usage_calendar_month": "Int64",
            "usage_budget_month_order": "Int64",
        }
    )
    return df

def transform_printer_solution(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_data_type(df)

    return df