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
        postgresql-client-15 \
        postgresql-client-16 \
        postgresql-client-17 \
        postgresql-client-18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN for version in 15 16 17 18; do \
      printf '#!/bin/sh\nexec /usr/lib/postgresql/%s/bin/pg_dump "$@"\n' "$version" \
        > "/usr/local/bin/pg_dump${version}"; \
      chmod +x "/usr/local/bin/pg_dump${version}"; \
    done

# ===============================
ARG MYSQL56_VERSION=5.6.51
ARG MYSQL56_URL=https://downloads.mysql.com/archives/get/p/23/file/mysql-5.6.51-linux-glibc2.12-x86_64.tar.gz

RUN mkdir -p /opt/mysql56 \
    && cd /tmp \
    && wget -q "${MYSQL56_URL}" -O mysql56.tar.gz \
    && tar -xzf mysql56.tar.gz \
       --strip-components=1 -C /opt/mysql56 \
    && rm -f mysql56.tar.gz \
    \
    # Keep only mysqldump executable
    && find /opt/mysql56/bin -type f ! -name mysqldump -delete \
    && find /opt/mysql56/bin -type l -delete

# MySQL 5.6 wrapper
RUN printf '#!/bin/sh\n\
export LD_LIBRARY_PATH="/opt/mysql56/lib:/opt/mysql56/lib/private:${LD_LIBRARY_PATH:-}"\n\
exec /opt/mysql56/bin/mysqldump "$@"\n' \
    > /usr/local/bin/mysqldump56 \
    && chmod +x /usr/local/bin/mysqldump56
ARG MYSQL80_VERSION=8.0.46
ARG MYSQL80_URL=https://dev.mysql.com/get/Downloads/MySQL-8.0/mysql-8.0.46-linux-glibc2.17-x86_64-minimal.tar.xz

RUN mkdir -p /opt/mysql80 \
    && cd /tmp \
    && wget -q "${MYSQL80_URL}" -O mysql80.tar.xz \
    && tar -xJf mysql80.tar.xz \
       --strip-components=1 -C /opt/mysql80 \
    && rm -f mysql80.tar.xz \
    \
    # Keep only mysqldump executable
    && find /opt/mysql80/bin -type f ! -name mysqldump -delete \
    && find /opt/mysql80/bin -type l -delete

# MySQL 8.0 wrapper
RUN printf '#!/bin/sh\n\
export LD_LIBRARY_PATH="/opt/mysql80/lib:/opt/mysql80/lib/private:${LD_LIBRARY_PATH:-}"\n\
exec /opt/mysql80/bin/mysqldump "$@"\n' \
    > /usr/local/bin/mysqldump80 \
    && chmod +x /usr/local/bin/mysqldump80

# ===============================
# MySQL 8.4 LTS mysqldump
# ===============================
ARG MYSQL84_VERSION=8.4.13
ARG MYSQL84_URL=https://dev.mysql.com/get/Downloads/MySQL-8.4/mysql-8.4.13-linux-glibc2.28-x86_64-minimal.tar.xz

RUN mkdir -p /opt/mysql84 \
    && cd /tmp \
    && wget -q "${MYSQL84_URL}" -O mysql84.tar.xz \
    && tar -xJf mysql84.tar.xz \
       --strip-components=1 -C /opt/mysql84 \
    && rm -f mysql84.tar.xz \
    \
    # Keep only mysqldump executable
    && find /opt/mysql84/bin -type f ! -name mysqldump -delete \
    && find /opt/mysql84/bin -type l -delete

# MySQL 8.4 wrapper
RUN printf '#!/bin/sh\n\
export LD_LIBRARY_PATH="/opt/mysql84/lib:/opt/mysql84/lib/private:${LD_LIBRARY_PATH:-}"\n\
exec /opt/mysql84/bin/mysqldump "$@"\n' \
    > /usr/local/bin/mysqldump84 \
    && chmod +x /usr/local/bin/mysqldump84


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