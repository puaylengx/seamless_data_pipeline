FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 1) ติดตั้งเครื่องมือพื้นฐาน (ไม่มี apt-transport-https เพราะ apt รองรับ https ในตัวแล้ว)
RUN apt-get update && apt-get install -y \
    curl gnupg ca-certificates unixodbc-dev build-essential \
    && rm -rf /var/lib/apt/lists/*

# 2) เพิ่ม Microsoft repo ด้วย keyring (แทน apt-key)
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
    > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Cloud Run Jobs: รันสคริปต์แล้วจบ
CMD ["python", "-m", "education.student_information.pipeline"]