# dags/src/helpers/logger.py
from __future__ import annotations
import logging

def get_logger(name: str = "logger") -> logging.Logger:
    """
    คืน logger ที่ผูกกับ task logger ของ Airflow เพื่อลดความเสี่ยง handler ซ้อน/ระดับเพี้ยน
    - ไม่สร้าง StreamHandler เอง
    - ไม่ไปเปลี่ยน handler tree ของ Airflow
    """
    # 1) อ้างอิงจาก task logger โดยตรง แล้วแตกเป็น child logger ของเรา
    base = logging.getLogger("airflow.task")   # หรือ "airflow"
    logger = base.getChild(name)               # ได้ชื่อ "airflow.task.logger"

    # 2) ตั้งระดับที่ตัว logger (Airflow จะยังคุม handler ให้)
    logger.setLevel(logging.INFO)

    # 3) สำคัญ: อย่าเพิ่ม handler เอง และอย่าไปแตะ propagate ที่นี่
    #    ปล่อยให้ Airflow จัดการ handler/formatter เองในบริบทของ task
    return logger
