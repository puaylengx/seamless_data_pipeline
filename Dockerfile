FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System deps for pyodbc + Microsoft ODBC 18
RUN apt-get update && apt-get install -y \
    curl gnupg apt-transport-https ca-certificates unixodbc-dev build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Cloud Run Jobs: รันสคริปต์แล้วจบ (ไม่ต้องเปิดพอร์ต)
CMD ["python", "-m", "education.student_information.pipeline"]