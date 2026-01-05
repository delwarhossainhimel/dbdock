#!/bin/bash

DB_DIR="/app/instances"
DB_FILE="$DB_DIR/dbDock.db"
DEFAULT_DB="/app/default/dbDock.db"

echo "🔍 Checking database..."

# Always ensure directory exists
mkdir -p "$DB_DIR"

# Check if mounted volume is empty
if [ -z "$(ls -A $DB_DIR 2>/dev/null)" ]; then
    echo "Mounted volume is empty"
    
    if [ -f "$DEFAULT_DB" ]; then
        echo "Copying default database to mounted volume..."
        cp "$DEFAULT_DB" "$DB_FILE"
        echo "Database copied to mounted volume"
    else
        echo "ERROR: Default database not found at $DEFAULT_DB"
        ls -la /app/default/ 2>/dev/null || echo "Default directory not found"
        exit 1
    fi
else
    echo "Mounted volume contains data:"
    ls -la "$DB_DIR/"
    
    # Check if database file exists in mounted volume
    if [ -f "$DB_FILE" ]; then
        echo "Using existing database from mounted volume"
    else
        echo "Mounted volume has files but no dbDock.db"
        if [ -f "$DEFAULT_DB" ]; then
            cp "$DEFAULT_DB" "$DB_FILE"
            echo "Created database in mounted volume"
        fi
    fi
fi

# Set permissions (important for SQLite)
chmod 666 "$DB_FILE" 2>/dev/null || true
chmod 777 "$DB_DIR" 2>/dev/null || true

echo "Final database location: $DB_FILE"
echo "Database size: $(du -h "$DB_FILE" 2>/dev/null | cut -f1 || echo '0B')"

echo "Starting application..."
#exec gunicorn -w 1 -b 0.0.0.0:5000 app:app
exec gunicorn \
  -w 1 \
  -b 0.0.0.0:5000 \
  --timeout 18000 \
  --graceful-timeout 18000 \
  app:app
