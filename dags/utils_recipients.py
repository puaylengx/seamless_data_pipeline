import os, json
from airflow.models import Variable

def resolve_recipients():
    # 1) ลองอ่านจาก ENV ก่อน (ถ้ามี)
    raw = os.getenv("ALERT_EMAILS")
    if raw:
        try:
            return [e.strip() for e in raw.split(",") if e.strip()]
        except:
            pass

    # 2) ถ้า ENV ไม่มี → ใช้ Variable
    try:
        raw = Variable.get("ALERT_EMAILS")
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return [e.strip() for e in data]
        except:
            return [e.strip() for e in raw.split(",") if e.strip()]
    except Exception:
        pass

    # 3) fallback ปลอดภัย
    return ["juntima.nuc@mahidol.ac.th"]