# ===============================
# Stage 1: Build base image
# ===============================
FROM python:3.12.3-slim AS base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

# Set working directory
WORKDIR /app

# Install system dependencies (MySQL + PostgreSQL clients + build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-mysql-client \
    postgresql-client \
    build-essential \
    libpq-dev \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy requirement file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn
# Dockerfile snippet
# Dockerfile snippet

# Copy project files
COPY . .
COPY ./default/dbDock.db /app/default/dbDock.db
RUN chmod +x entrypoint.sh
# Expose the port
EXPOSE 5000

# Healthcheck (optional)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD curl -f http://localhost:5000/health || exit 1

# ===============================
# Stage 2: Run Gunicorn
# ===============================
#CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:5000", "app:app"]
# Entrypoint
ENTRYPOINT ["sh", "/app/entrypoint.sh"]
