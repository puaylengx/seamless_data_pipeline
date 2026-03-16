# dags/_callbacks.py
from __future__ import annotations
import os, json, html
from typing import List
from airflow.utils.email import send_email
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State

# ------------------------------
# Recipients resolver (ENV -> Variable -> fallback)
# ------------------------------
def _recipients() -> List[str]:
    raw = os.getenv("ALERT_EMAILS", "")
    if raw.strip():
        return [e.strip() for e in raw.replace(";", ",").split(",") if e.strip()]
    try:
        from airflow.models import Variable
        raw = Variable.get("ALERT_EMAILS", default_var="")
        if raw.strip():
            try:
                data = json.loads(raw)
                if isinstance(data, list):
                    return [str(x).strip() for x in data if str(x).strip()]
            except Exception:
                return [s.strip() for s in raw.replace(";", ",").split(",") if s.strip()]
    except Exception:
        pass
    return ["fallback@your-domain.com"]  # fallback

# ------------------------------
# Task-level callbacks
# ------------------------------
def on_task_failure_callback(context):
    ti = context["task_instance"]
    subject = f"[Airflow][FAILED] {ti.dag_id}.{ti.task_id} (run: {ti.run_id}, try: {ti.try_number})"
    html_body = f"""
        <h3>❌ Task Failed</h3>
        <ul>
        <li><b>DAG</b>: {html.escape(ti.dag_id)}</li>
        <li><b>Task</b>: {html.escape(ti.task_id)}</li>
        <li><b>Run ID</b>: {html.escape(ti.run_id)}</li>
        <li><b>Try</b>: {ti.try_number}</li>
        <li><b>Log</b>: <a href="{ti.log_url}">Open</a></li>
        </ul>
    """
    send_email(to=_recipients(), subject=subject, html_content=html_body)

def on_task_success_callback(context):
    ti = context["task_instance"]

    # ✅ ดึง XCom ของ task (TaskFlow จะได้เป็น dict)
    x = ti.xcom_pull(task_ids=ti.task_id)
    message, rows, path = None, None, None
    if isinstance(x, dict):
        message = x.get("message")
        rows = x.get("rows")
        path = x.get("path")

    # fallback เผื่อ push แยก key
    if message is None:
        message = ti.xcom_pull(task_ids=ti.task_id, key="message")
    if rows is None:
        rows = ti.xcom_pull(task_ids=ti.task_id, key="rows")
    if path is None:
        path = ti.xcom_pull(task_ids=ti.task_id, key="path")

    # สร้างบรรทัดสรุป
    if message:
        summary_line = str(message)
    else:
        parts = []
        if rows is not None: parts.append(f"rows={rows}")
        if path: parts.append(f"path={path}")
        summary_line = ("; ".join(parts)) if parts else "No summary message was produced."

    subject = f"[Airflow][SUCCESS] {ti.dag_id}.{ti.task_id} (run: {ti.run_id})"
    html_body = f"""
        <h3>✅ Task Success</h3>
        <ul>
        <li><b>DAG</b>: {html.escape(ti.dag_id)}</li>
        <li><b>Task</b>: {html.escape(ti.task_id)}</li>
        <li><b>Run ID</b>: {html.escape(ti.run_id)}</li>
        <li><b>Summary</b>:
            <pre style="display:inline-block;margin:0">{html.escape(summary_line)}</pre>
        </li>
        <li><b>Log</b>: <a href="{ti.log_url}">Open</a></li>
        </ul>
    """
    send_email(to=_recipients(), subject=subject, html_content=html_body)

# ------------------------------
# DAG-level success summary (รวม XCom)
# ------------------------------
def on_dag_success_callback(context):
    """
    เรียกเมื่อทั้ง DAG สำเร็จ: รวบรวม XCom['message'] ของ extract/transform/validate/load แล้วส่งอีเมล
    """
    dag_run: DagRun = context["dag_run"]
    ti_map: dict[str, TaskInstance] = {ti.task_id: ti for ti in dag_run.get_task_instances()}

    task_ids = ["extract", "transform", "validate", "load"]
    lines: List[str] = []

    for tid in task_ids:
        ti = ti_map.get(tid)
        if ti and ti.state == State.SUCCESS:
            x = ti.xcom_pull(task_ids=tid)  # expect dict or str
            msg = None
            if isinstance(x, dict):
                msg = x.get("message")
            elif isinstance(x, str):
                msg = x
            if msg:
                lines.append(html.escape(str(msg)))

    if not lines:
        lines = ["No summary messages were produced."]

    # ลิงก์ไปหน้า Run ของ DAG
    base_url = os.getenv("AIRFLOW__WEBSERVER__BASE_URL", "http://localhost:8080")
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    run_url = f"{base_url}/dags/{dag_id}/grid?dag_run_id={run_id}"

    subject = f"Airflow Summary: {dag_id} ({run_id})"
    html_body = f"""
        <h3>Pipeline finished</h3>
        <p>DAG: {html.escape(dag_id)} / Run: {html.escape(run_id)}</p>
        <p>Start: {html.escape(str(context.get('data_interval_start')))} — End: {html.escape(str(context.get('data_interval_end')))}</p>
        <pre style="font-size:13px; line-height:1.35">{'\n'.join(lines)}</pre>
        <p><a href="{run_url}">Open DAG Run</a></p>
    """
    send_email(to=_recipients(), subject=subject, html_content=html_body)