# ===============================
# Stage 1: Build base image
# ===============================
FROM python:3.13.15-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Dhaka

WORKDIR /app

# Install system deps + PostgreSQL 17 client + timezone
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    gnupg \
    tzdata \
    build-essential \
    default-mysql-client \
    libpq-dev \
    && ln -snf /usr/share/zoneinfo/Asia/Dhaka /etc/localtime \
    && echo "Asia/Dhaka" > /etc/timezone \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
       | gpg --dearmor -o /usr/share/keyrings/postgres.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/postgres.gpg] \
       http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
       > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
       postgresql-client-18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ARG MYSQL56_VERSION=5.6.51
ARG MYSQL56_URL=https://downloads.mysql.com/archives/get/p/23/file/mysql-5.6.51-linux-glibc2.12-x86_64.tar.gz

RUN mkdir -p /opt/mysql56 \
    && cd /tmp \
    && wget -q "${MYSQL56_URL}" -O mysql56.tar.gz \
    && tar -xzf mysql56.tar.gz \
    && mv mysql-5.6.51-linux-glibc2.12-x86_64/* /opt/mysql56/ \
    && rm -rf mysql56.tar.gz mysql-5.6.51-linux-glibc2.12-x86_64

# Create dedicated MySQL 5.6 mysqldump command
RUN ln -s /opt/mysql56/bin/mysqldump /usr/local/bin/mysqldump56 \
    && ln -s /opt/mysql56/bin/mysql /usr/local/bin/mysql56
# Install Python deps
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn

# Copy project files
COPY . .

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -f http://localhost:5000/health || exit 1

CMD ["python", "-m", "gunicorn", "--workers", "1", "--bind", "0.0.0.0:5000", "--access-logfile", "-", "--error-logfile", "-", "app:app"]