# dags/src/education/student_information/loaders/student_information_bigquery.py
from __future__ import annotations
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
import pandas as pd

def load_dataframe_to_bq(
    df: pd.DataFrame,
    project_id: str,
    dataset: str,
    table: str,
    gcp_conn_id: str,
    write_disposition: str = "WRITE_TRUNCATE",  # or WRITE_APPEND
    location: str = "asia-southeast1",
) -> None:
    """
    โหลด DataFrame เข้า BigQuery โดยใช้ BigQueryHook.get_client()
    """
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, location=location)
    client: bigquery.Client = hook.get_client(project_id=project_id)

    table_id = f"{project_id}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,   # ให้ BQ ใช้ schema/สเปกของตารางที่มีอยู่
    )

    # โหลดจาก DataFrame
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # รอจนจบ