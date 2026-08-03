# finance_invoice/emailer_smtplib.py
import os
import time
import smtplib
import logging
import html as _html
from dataclasses import dataclass
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr

logger = logging.getLogger("airflow.task")


@dataclass
class EmailConfig:
    host: str
    port: int
    username: str
    password: str
    alert_email: str
    from_name: str = "Airflow Notification"

    @property
    def recipients(self) -> list[str]:
        return [r.strip() for r in (self.alert_email or "").split(",") if r.strip()]

    def is_ready(self) -> bool:
        return all(
            [self.host, self.port, self.username, self.password, self.recipients]
        )


def _load_from_env() -> EmailConfig:
    return EmailConfig(
        host=os.getenv("SMTP_HOST", "").strip(),
        port=int(os.getenv("SMTP_PORT", "0") or 0),
        username=os.getenv("SMTP_USER", "").strip(),
        password=os.getenv("SMTP_PASSWORD", "").strip(),
        alert_email=os.getenv("ALERT_EMAILS", "").strip(),
        from_name=os.getenv("SMTP_MAIL_FROM", "Airflow Notification").strip(),
    )


def _load_from_airflow_connection(conn_id: str = "smtp_default") -> EmailConfig:
    from airflow.hooks.base import BaseHook  # Airflow runtime only

    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}

    # ✅ รองรับหลาย key เพื่อไม่จุกจิก
    alert_email = (
        os.getenv("ALERT_EMAILS", "").strip()
        or os.getenv("MAIL_TO", "").strip()
        or str(extra.get("alert_email", "")).strip()
        or str(extra.get("mail_to", "")).strip()
    )
    from_name = (
        os.getenv("SMTP_MAIL_FROM", "").strip()
        or os.getenv("MAIL_FROM_NAME", "").strip()
        or str(extra.get("from_name", "")).strip()
        or "Airflow Notification"
    )

    return EmailConfig(
        host=(conn.host or "").strip(),
        port=int(conn.port or 0),
        username=(conn.login or "").strip(),
        password=(conn.password or "").strip(),
        alert_email=alert_email,
        from_name=from_name,
    )


def load_email_config_from_env(conn_id: str = "smtp_default") -> EmailConfig:
    """
    ✅ ไม่กระทบ DAG: ยังเรียกชื่อเดิมได้
    1) พยายามอ่านจาก ENV ก่อน (local/dev)
    2) ถ้า ENV ไม่ครบ → fallback ไป Airflow Connection (prod)
    """
    cfg = _load_from_env()
    if cfg.is_ready():
        logger.info("📧 Email config loaded from ENV")
        return cfg

    # ENV ไม่ครบ → ไป Connection
    try:
        cfg2 = _load_from_airflow_connection(conn_id)
        if cfg2.is_ready():
            logger.info("📧 Email config loaded from Airflow Connection: %s", conn_id)
            return cfg2
    except Exception:
        logger.exception(
            "⚠️ Failed to load email config from Airflow Connection: %s", conn_id
        )

    # ยังไม่ครบ → คืนของเดิม (จะทำให้ send_summary_email ข้ามส่ง + log เตือน)
    logger.warning("⚠️ Email config not ready (ENV + Connection). Skipping email.")
    return cfg


def _send_html(subject: str, body: str, email_cfg: EmailConfig):
    """ส่ง HTML mail พร้อม retry 3 ครั้ง (ใช้ร่วมกันทั้งเมลสรุปและเมลแจ้ง error)"""
    msg = MIMEMultipart("alternative")
    msg["From"] = formataddr((email_cfg.from_name, email_cfg.username))
    msg["To"] = email_cfg.alert_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html", "utf-8"))

    for attempt in range(3):
        try:
            if email_cfg.port == 465:
                with smtplib.SMTP_SSL(
                    email_cfg.host, email_cfg.port, timeout=10
                ) as server:
                    server.login(email_cfg.username, email_cfg.password)
                    server.sendmail(
                        email_cfg.username, email_cfg.recipients, msg.as_string()
                    )
            else:
                with smtplib.SMTP(email_cfg.host, email_cfg.port, timeout=10) as server:
                    server.starttls()
                    server.login(email_cfg.username, email_cfg.password)
                    server.sendmail(
                        email_cfg.username, email_cfg.recipients, msg.as_string()
                    )

            logger.info("📧 ส่งอีเมลสำเร็จ → %s", email_cfg.alert_email)
            return
        except Exception:
            if attempt < 2:
                logger.warning("⚠️ ส่งอีเมลล้มเหลว (%s/3) กำลังลองใหม่...", attempt + 1)
                time.sleep(3)
            else:
                logger.exception("❌ ส่งอีเมลไม่สำเร็จหลัง retry 3 ครั้ง")


def send_summary_email(
    result: dict, email_cfg: EmailConfig, updated_rows_sample: list | None = None
):
    # ใช้ env เดิม
    if not email_cfg.is_ready():
        logger.warning("⚠️ ข้ามการส่งอีเมล: SMTP/ENV ไม่ครบ")
        return

    # --------- Data ----------

    pipeline_name = result.get("subject", "Pipeline")
    run_date = result.get("run_date", "")
    subject = f"📊 สรุป {pipeline_name} — {run_date}"

    # subject    = f"📊 สรุป {result.get('subject')} — {result.get('run_date')}"
    inserted = int(result.get("inserted", 0) or 0)
    updated = int(result.get("updated", 0) or 0)
    duration = result.get("duration_sec", 0)
    rows = int(result.get("rows_total", result.get("rows", 0)) or 0)
    batches = int(result.get("batches_total", 0) or 0)
    table = result.get("target_table", result.get("target", "N/A"))
    status_txt = (result.get("status") or "").upper()
    log_link = result.get("log_file", "")
    audit_link = result.get("audit_file", "")

    # สีป้ายสถานะ
    status_bg = (
        "#198754"
        if status_txt in ("SUCCESS", "OK")
        else ("#dc3545" if status_txt in ("FAILED", "FAIL") else "#6c757d")
    )

    # --------- Build sample rows (max 10) ----------
    rows_html = ""

    # ✅ function เลือก identifier
    def _pick_identifier(u: dict):
        student_code = u.get("studentCode")
        if student_code is not None and str(student_code).strip() != "":
            return str(student_code), "StudentCode"

        # fallback → invoice
        invoice = u.get("invoiceId")
        if invoice is not None and str(invoice).strip() != "":
            return str(invoice), "Invoice"

        return "N/A", "Invoice"

    # ✅ เลือก header จากตัวแรก
    id_label = "Invoice"

    if updated_rows_sample:
        for u in updated_rows_sample:
            _, id_label = _pick_identifier(u)
            break

    # ✅ build table
    if updated_rows_sample:
        for u in updated_rows_sample[:10]:
            ident_val, _ = _pick_identifier(u)
            ident_val = _html.escape(ident_val)

            for col, diff in (u.get("changes") or {}).items():
                col = _html.escape(str(col))
                old_val = _html.escape(str((diff or {}).get("old", "") or ""))
                new_val = _html.escape(str((diff or {}).get("new", "") or ""))

                rows_html += f"""
                    <tr>
                        <td style="padding:10px 12px; border-bottom:1px solid #eee; white-space:nowrap;">{ident_val}</td>
                        <td style="padding:10px 12px; border-bottom:1px solid #eee;">{col}</td>
                        <td style="padding:10px 12px; border-bottom:1px solid #eee; color:#6c757d;">{old_val}</td>
                        <td style="padding:10px 12px; border-bottom:1px solid #eee; color:#198754; font-weight:600;">{new_val}</td>
                    </tr>
                """

    # --------- HTML email ----------
    body = f"""\
      <!DOCTYPE html>
      <html lang="th">
      <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width"/>
      <title>{_html.escape(str(result.get('subject') or 'Pipeline'))} Summary</title>
      <style>
      @media only screen and (max-width:600px) {{
        .container {{ width: 100% !important; padding: 16px !important; }}
        .grid-2 {{ display:block !important; }}
        .metric {{ width: 100% !important; display:block !important; margin-bottom:10px !important; }}
        .btn {{ display:block !important; width: 100% !important; text-align:center !important; margin-bottom:8px !important; }}
      }}
      </style>
      </head>
      <body style="margin:0; padding:0; background:#f6f7fb; font-family:Segoe UI, Arial, Helvetica, sans-serif; color:#212529;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f6f7fb; padding:20px 0;">
        <tr>
          <td align="center">
            <table role="presentation" class="container" width="720" cellpadding="0" cellspacing="0"
                  style="background:#ffffff; width:720px; max-width:720px; border-radius:12px; box-shadow:0 6px 18px rgba(0,0,0,0.06); overflow:hidden;">
              <tr>
                <td style="background:#0d6efd; padding:20px 24px; color:#ffffff;">
                  <div style="font-size:20px; font-weight:700;">{_html.escape(str(result.get('subject') or 'Pipeline'))} — รายงานสรุป</div>
                  <div style="opacity:0.9; font-size:13px; margin-top:4px;">เวลาเริ่มรัน: {_html.escape(str(result.get('run_date') or ''))}</div>
                </td>
              </tr>

              <tr>
                <td style="padding:16px 24px 0 24px;">
                  <span style="display:inline-block; background:{status_bg}; color:#fff; font-size:12px; font-weight:700; letter-spacing:.3px; padding:6px 10px; border-radius:999px; text-transform:uppercase;">
                    {_html.escape(status_txt or 'N/A')}
                  </span>
                  <span style="display:inline-block; margin-left:12px; color:#6c757d; font-size:12px;">
                    ตาราง: <strong style="color:#212529;">{_html.escape(str(table or 'N/A'))}</strong>
                  </span>
                </td>
              </tr>

              <tr>
                <td style="padding:12px 24px 0 24px;">
                  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" class="grid-2" style="border-collapse:separate; border-spacing:12px 12px;">
                    <tr>
                      <td class="metric" style="background:#f1f4ff; border:1px solid #e1e7ff; border-radius:10px; padding:14px;">
                        <div style="font-size:12px; color:#4a60a1;">INSERT</div>
                        <div style="font-size:22px; font-weight:800; margin-top:4px;">{inserted:,}</div>
                      </td>
                      <td class="metric" style="background:#f0fff5; border:1px solid #d7f5e1; border-radius:10px; padding:14px;">
                        <div style="font-size:12px; color:#2f7a47;">UPDATE</div>
                        <div style="font-size:22px; font-weight:800; margin-top:4px;">{updated:,}</div>
                      </td>
                    </tr>
                    <tr>
                      <td class="metric" style="background:#fff8e6; border:1px solid #ffedc2; border-radius:10px; padding:14px;">
                        <div style="font-size:12px; color:#a36316;">รวมแถว (batches)</div>
                        <div style="font-size:18px; font-weight:700; margin-top:4px;">{rows:,}
                          <span style="font-size:12px; color:#6c757d;">({batches} ชุด)</span>
                        </div>
                      </td>
                      <td class="metric" style="background:#eef5f9; border:1px solid #d7e7f0; border-radius:10px; padding:14px;">
                        <div style="font-size:12px; color:#2f5266;">ระยะเวลา</div>
                        <div style="font-size:18px; font-weight:700; margin-top:4px;">{duration} วินาที</div>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>

              <tr>
                <td style="padding:4px 24px 0 24px;">
                  <table role="presentation" cellpadding="0" cellspacing="0">
                    <tr>
                      <td>
                        <a class="btn" href="{_html.escape(str(log_link or ''))}" style="display:inline-block; background:#0d6efd; color:#fff; text-decoration:none; padding:10px 14px; border-radius:8px; font-weight:600; font-size:13px; border:1px solid #0d6efd;">📄 เปิด Log</a>
                      </td>
                      <td width="8"></td>
                      <td>
                        <a class="btn" href="{_html.escape(str(audit_link or ''))}" style="display:inline-block; background:#6c757d; color:#fff; text-decoration:none; padding:10px 14px; border-radius:8px; font-weight:600; font-size:13px; border:1px solid #6c757d;">🧾 เปิด Audit</a>
                      </td>
                    </tr>
                  </table>
                  <div style="color:#6c757d; font-size:12px; margin-top:8px;">* หากลิงก์ไม่เปิด (เช่น path ภายในเครื่องเซิร์ฟเวอร์) โปรดเข้าไปที่เครื่องที่รันจ็อบแล้วเปิดไฟล์ตาม path</div>
                </td>
              </tr>

              <tr><td style="padding:16px 24px 0 24px;"><hr style="border:none; border-top:1px solid #e9ecef; margin:0;"></td></tr>

              {("" if not rows_html else f"""
              <tr>
                <td style="padding:16px 24px 8px 24px;">
                  <div style="font-weight:700; margin-bottom:8px; color:#212529;">📝 ตัวอย่างรายการ UPDATE (สูงสุด 10 รายการ)</div>
                  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; border:1px solid #e9ecef; border-radius:8px; overflow:hidden;">
                    <thead>
                      <tr style="background:#f8f9fa;">
                        <th align="left" style="padding:10px 12px; font-size:12px; color:#495057; border-bottom:1px solid #e9ecef; white-space:nowrap;">{_html.escape(id_label)} </th>
                        <th align="left" style="padding:10px 12px; font-size:12px; color:#495057; border-bottom:1px solid #e9ecef;">Field</th>
                        <th align="left" style="padding:10px 12px; font-size:12px; color:#495057; border-bottom:1px solid #e9ecef;">Old</th>
                        <th align="left" style="padding:10px 12px; font-size:12px; color:#495057; border-bottom:1px solid #e9ecef;">New</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows_html}
                    </tbody>
                  </table>
                  <div style="color:#6c757d; font-size:12px; margin-top:6px;">* ดูรายละเอียดทั้งหมดในไฟล์ Audit</div>
                </td>
              </tr>
              """)}

              <tr>
                <td style="padding:18px 24px 22px 24px; color:#6c757d; font-size:12px;">
                  รันโดยระบบอัตโนมัติ — {_html.escape(str(result.get('subject') or 'Pipeline'))} Job<br>
                  เวลาเสร็จสิ้น: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
      </body>
      </html>
    """

    _send_html(subject, body, email_cfg)


# =========================================================
# Failure alert (ใช้เป็น on_failure_callback ของทุก DAG)
# =========================================================
def _failure_log_url(ti) -> str:
    """หา URL ของ log ให้ดีที่สุดเท่าที่ทำได้ (Airflow 2/3 คนละ attribute)"""
    url = getattr(ti, "log_url", "") or ""
    if url:
        return url

    base = (
        os.getenv("AIRFLOW__API__BASE_URL", "")
        or os.getenv("AIRFLOW__WEBSERVER__BASE_URL", "")
        or os.getenv("AIRFLOW_BASE_URL", "")
    ).rstrip("/")
    dag_id = getattr(ti, "dag_id", "")
    if not (base and dag_id):
        return ""
    return f"{base}/dags/{dag_id}/runs/{getattr(ti, 'run_id', '')}/tasks/{getattr(ti, 'task_id', '')}"


def build_failure_email(context: dict) -> tuple[str, str]:
    """สร้าง (subject, html) จาก Airflow task context — แยกออกมาเพื่อเทสต์ได้โดยไม่ต้องส่งเมล"""
    ti = context.get("ti") or context.get("task_instance")
    exc = context.get("exception")

    dag_id = getattr(ti, "dag_id", "") or str(context.get("dag_id", "unknown"))
    task_id = getattr(ti, "task_id", "") or "unknown"
    run_id = getattr(ti, "run_id", "") or str(context.get("run_id", ""))
    try_number = getattr(ti, "try_number", "") or context.get("try_number", "")
    when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_url = _failure_log_url(ti)

    err_type = type(exc).__name__ if exc else "Unknown"
    err_msg = str(exc) if exc else "ไม่มีรายละเอียด error"
    if len(err_msg) > 4000:
        err_msg = err_msg[:4000] + "\n... (ตัดข้อความ)"

    subject = f"🔴 FAILED {dag_id}.{task_id} — {when}"

    rows = "".join(
        f"""<tr>
              <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#6c757d; white-space:nowrap;">{_html.escape(k)}</td>
              <td style="padding:8px 12px; border-bottom:1px solid #eee; font-weight:600;">{_html.escape(str(v))}</td>
            </tr>"""
        for k, v in [
            ("DAG", dag_id),
            ("Task", task_id),
            ("Run ID", run_id),
            ("ครั้งที่ลอง", try_number),
            ("เวลา", when),
        ]
    )

    log_btn = (
        f'<a href="{_html.escape(log_url)}" style="display:inline-block; background:#dc3545; color:#fff; '
        f'text-decoration:none; padding:10px 14px; border-radius:8px; font-weight:600; font-size:13px;">📄 เปิด Log</a>'
        if log_url
        else '<div style="color:#6c757d; font-size:12px;">* ดู log ได้ที่ Airflow UI</div>'
    )

    body = f"""\
<!DOCTYPE html>
<html lang="th">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width"/><title>Task Failed</title></head>
<body style="margin:0; padding:0; background:#f6f7fb; font-family:Segoe UI, Arial, Helvetica, sans-serif; color:#212529;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f6f7fb; padding:20px 0;">
  <tr><td align="center">
    <table role="presentation" width="720" cellpadding="0" cellspacing="0"
           style="background:#fff; width:720px; max-width:720px; border-radius:12px; box-shadow:0 6px 18px rgba(0,0,0,.06); overflow:hidden;">
      <tr>
        <td style="background:#dc3545; padding:20px 24px; color:#fff;">
          <div style="font-size:20px; font-weight:700;">Task Failed</div>
          <div style="opacity:.9; font-size:13px; margin-top:4px;">{_html.escape(dag_id)} → {_html.escape(task_id)}</div>
        </td>
      </tr>
      <tr>
        <td style="padding:16px 24px 0 24px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
                 style="border-collapse:collapse; border:1px solid #e9ecef; border-radius:8px; overflow:hidden; font-size:13px;">
            {rows}
          </table>
        </td>
      </tr>
      <tr>
        <td style="padding:16px 24px 0 24px;">
          <div style="font-weight:700; margin-bottom:8px;">⚠️ {_html.escape(err_type)}</div>
          <pre style="background:#fff5f5; border:1px solid #ffd7d7; border-radius:8px; padding:14px; margin:0;
                      font-size:12px; line-height:1.6; white-space:pre-wrap; word-break:break-word; color:#842029;">{_html.escape(err_msg)}</pre>
        </td>
      </tr>
      <tr><td style="padding:16px 24px 0 24px;">{log_btn}</td></tr>
      <tr>
        <td style="padding:18px 24px 22px 24px; color:#6c757d; font-size:12px;">
          แจ้งเตือนอัตโนมัติจาก Airflow — on_failure_callback
        </td>
      </tr>
    </table>
  </td></tr>
</table>
</body>
</html>
"""
    return subject, body


def task_failure_alert(context: dict) -> None:
    """
    ใช้เป็น on_failure_callback ใน default_args ของ DAG:

        default_args={"retries": 2, "on_failure_callback": task_failure_alert}

    ยิงเมลเมื่อ task fail จริง (หลัง retry หมดแล้ว) ต่างจาก task notify ปกติ
    ที่จะถูก skip ไปเมื่อ task ต้นทางพัง

    callback นี้ต้องไม่ throw เด็ดขาด ไม่งั้นจะกลบ error ตัวจริง
    """
    try:
        subject, body = build_failure_email(context)
        cfg = load_email_config_from_env()
        if not cfg.is_ready():
            logger.warning("⚠️ ข้ามการส่งเมลแจ้ง error: SMTP config ไม่ครบ")
            return
        _send_html(subject, body, cfg)
    except Exception:
        logger.exception("❌ task_failure_alert ล้มเหลว (ไม่กระทบ error เดิมของ task)")
