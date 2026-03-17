# finance_invoice/audit.py
import json
from datetime import datetime
from pathlib import Path

AUDIT_DIR = Path("/opt/airflow/data/finance_invoice/audit")
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

AUDIT_FILE = AUDIT_DIR / f"audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"

def write_audit_line(payload: dict):
    with open(AUDIT_FILE, "a", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, default=str)
        f.write("\n")
