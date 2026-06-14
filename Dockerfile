FROM python:3.12-slim

# Cài đặt các thư viện hệ thống cần thiết (để build psycopg2-binary, git, v.v.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập thư mục làm việc và PYTHONPATH
WORKDIR /app
ENV PYTHONPATH=/app
ENV AIRFLOW_HOME=/app

# Sao chép và cài đặt các thư viện Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Expose cổng của Airflow Webserver
EXPOSE 8080

# Chạy Airflow ở chế độ standalone làm lệnh mặc định
CMD ["airflow", "standalone"]
