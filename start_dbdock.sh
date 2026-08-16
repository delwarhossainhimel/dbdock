#!/bin/bash
# dbDock startup script with Gunicorn

cd /backup/dbdock

# Load environment variables
if [ -f /etc/dbdock.env ]; then
    export $(grep -v '^#' /etc/dbdock.env | xargs)
fi

# Activate virtual environment
source venv/bin/activate

# Set environment variables
export PYTHONUNBUFFERED=1
export TZ=${TZ:-Asia/Dhaka}

# Create required directories
mkdir -p backup_logs tmp logs

# Clean up stale lock files
rm -f /tmp/backup_*.lock 2>/dev/null
rm -f /backup/dbdock/tmp/backup_*.lock 2>/dev/null

# Start with Gunicorn with log files
exec gunicorn --workers 1 \
    --bind 0.0.0.0:5000 \
    --access-logfile /backup/dbdock/logs/access.log \
    --error-logfile /backup/dbdock/logs/error.log \
    --log-level info \
    --timeout 300 \
    --capture-output \
    --enable-stdio-inheritance \
    app:app