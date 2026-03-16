from datetime import datetime

def build_summary_html(result: dict) -> str:
    status = (result.get("status") or "").upper()
    inserted = result.get("inserted", 0)
    updated = result.get("updated", 0)
    rows = result.get("rows", 0)
    target = result.get("target", "")
    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    status_color = "#198754" if status == "SUCCESS" else "#dc3545"

    return f"""
    <html>
      <body style="font-family:Segoe UI, Arial">
        <h2>📊 Finance Invoice ETL Summary</h2>

        <p>
          <b>Status:</b>
          <span style="color:white; background:{status_color};
                       padding:4px 8px; border-radius:6px;">
            {status}
          </span>
        </p>

        <table cellpadding="6" cellspacing="0" border="1" style="border-collapse:collapse;">
          <tr><td><b>Target Table</b></td><td>{target}</td></tr>
          <tr><td><b>Total Rows</b></td><td>{rows}</td></tr>
          <tr><td><b>Inserted</b></td><td>{inserted}</td></tr>
          <tr><td><b>Updated</b></td><td>{updated}</td></tr>
        </table>

        <p style="margin-top:16px; color:#666">
          Finished at: {finished_at}<br/>
          This email was sent automatically by Airflow.
        </p>
      </body>
    </html>
    """