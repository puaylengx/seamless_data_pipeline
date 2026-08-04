# dags/src/education/student_information/sensors/staging_student_ready.py
"""
เช็คว่า dbo.StagingStudent พร้อมใช้งานหรือยัง ก่อนจะไป extract

student_information.sql ทำ left join กับ dbo.StagingStudent เพื่อเอา programCode
มาทำ major_code ถ้าตารางยังไม่ถูกเติมข้อมูล join จะไม่ติดแล้วได้ NULL ทั้งก้อน
โดยที่ query ยังรันสำเร็จ (ดู commit 4e08d38)

วัดเป็น "สัดส่วนที่ match" แทนจำนวนแถวตายตัว เพื่อไม่ต้องมาปรับเลขทุกครั้งที่
จำนวนนักศึกษาเปลี่ยน
"""
from __future__ import annotations

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from src.helpers.logger import get_logger

logger = get_logger(__name__)

# ต้องตรงกับ WHERE ใน student_information.sql ไม่งั้นวัดคนละกลุ่มกับที่ pipeline ใช้จริง
ACTIVE_STATUSES = ("dm", "ex", "g", "la", "np", "prc", "pa", "rs", "s")
MIN_ACADEMIC_YEAR = "2016"

READINESS_SQL = f"""
SELECT
    COUNT(DISTINCT std.Code) AS total_students,
    COUNT(DISTINCT CASE
        WHEN NULLIF(LTRIM(RTRIM(ss.programCode)), '') IS NOT NULL
        THEN std.Code
    END) AS matched_students
FROM student.Students std
    LEFT JOIN student.AdmissionInformations ai ON std.Id = ai.StudentId
    LEFT JOIN dbo.Terms term ON ai.AdmissionTermId = term.Id
    LEFT JOIN dbo.StagingStudent ss ON std.Code = ss.studentCode
WHERE std.StudentStatus IN ({",".join(f"'{s}'" for s in ACTIVE_STATUSES)})
  AND term.AcademicYear >= '{MIN_ACADEMIC_YEAR}'
"""


def check_staging_student_ready(
    mssql_conn_id: str,
    min_match_ratio: float = 0.90,
) -> tuple[bool, dict]:
    """
    คืน (พร้อมหรือยัง, สถิติ)

    min_match_ratio  สัดส่วนขั้นต่ำของนักศึกษาที่ต้องหา programCode เจอใน StagingStudent
                     0.90 = ต้อง match อย่างน้อย 90% ถึงจะถือว่าตารางเติมเสร็จแล้ว

    ยกเว้น error ทิ้งไม่ได้ ต้องปล่อยให้ throw เพื่อให้ sensor รู้ว่าต่อ DB ไม่ได้
    """
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    row = hook.get_pandas_df(READINESS_SQL).iloc[0]

    total = int(row["total_students"])
    matched = int(row["matched_students"])
    ratio = (matched / total) if total else 0.0

    stats = {
        "total_students": total,
        "matched_students": matched,
        "match_ratio": round(ratio, 4),
        "min_match_ratio": min_match_ratio,
    }

    ready = total > 0 and ratio >= min_match_ratio
    logger.info(
        "%s StagingStudent readiness: %s/%s matched (%.1f%%, ต้องการ %.0f%%)",
        "✅" if ready else "⏳",
        matched,
        total,
        ratio * 100,
        min_match_ratio * 100,
    )
    return ready, stats
