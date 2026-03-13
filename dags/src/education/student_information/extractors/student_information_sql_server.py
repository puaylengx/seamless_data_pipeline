from __future__ import annotations
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd

def fetch_student_information( mssql_conn_id: str, sql_path: str, statuses: list[str], min_year: int) -> pd.DataFrame:
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)

    with open(sql_path, "r", encoding="utf-8") as f:
        query = f.read()

    # เติม placeholder สำหรับ IN (...)
    if not statuses:
        # กันเคสลิสต์ว่าง: ทำให้ WHERE in (...) ล้มเหลวอย่างปลอดภัย
        # หรือจะใช้ 'where 1=0' แทนก็ได้
        query = query.replace("/*__STATUSES__*/", "%s")
        params = {}
    else:
        placeholders = ", ".join(["%s"] * len(statuses))
        query = query.replace("/*__STATUSES__*/", placeholders)
        params = list(statuses)

    # ต่อท้ายค่าปีขั้นต่ำที่ปรากฏใน SQL (หลัง in (...))
    params.append(int(min_year))  # ให้ลำดับตรงกับ %s หลัง IN(...)

    # Debug: ลอง log query และจำนวน params
    # print(query); print(params)

    df = hook.get_pandas_df(sql=query, parameters=params)
    return df
