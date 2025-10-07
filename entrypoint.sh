#!/bin/bash

DB_DIR="/app/instances"
DB_FILE="$DB_DIR/dbDock.db"
DEFAULT_DB="/app/default/dbDock.db"

mkdir -p "$DB_DIR"

if [ ! -f "$DB_FILE" ]; then
    echo "🟢 Initializing database..."
    cp "$DEFAULT_DB" "$DB_FILE"
    echo "✅ Default DB copied to $DB_FILE"
fi

exec gunicorn -w 1 -b 0.0.0.0:5000 app:app

