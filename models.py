from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import enum

db = SQLAlchemy()

class DatabaseType(enum.Enum):
    MYSQL = 'mysql'
    POSTGRES = 'postgres'

class StorageType(enum.Enum):
    LOCAL = 'local'
    FTP = 'ftp'
    S3 = 's3'
    BLOB = 'blob'

class DatabaseServer(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    type = db.Column(db.Enum(DatabaseType), nullable=False)
    host = db.Column(db.String(200), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    username = db.Column(db.String(100), nullable=False)
    password = db.Column(db.String(200), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    
    def test_connection(self):
        try:
            if self.type == DatabaseType.MYSQL:
                import mysql.connector
                conn = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    user=self.username,
                    password=self.password
                )
                conn.close()
                return True, "Connection successful"
            elif self.type == DatabaseType.POSTGRES:
                import psycopg2
                conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.username,
                    password=self.password
                )
                conn.close()
                return True, "Connection successful"
        except Exception as e:
            return False, str(e)

class StorageLocation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    type = db.Column(db.Enum(StorageType), nullable=False)
    config = db.Column(db.Text, nullable=False)  # JSON string of configuration
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)

class BackupJob(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text)
    database_server_id = db.Column(db.Integer, db.ForeignKey('database_server.id'), nullable=False)
    databases = db.Column(db.Text, nullable=False)  # JSON string of selected databases
    storage_location_id = db.Column(db.Integer, db.ForeignKey('storage_location.id'), nullable=False)
    folder_path = db.Column(db.String(500), nullable=False)
    schedule_type = db.Column(db.String(20), nullable=False)  # daily, weekly, monthly
    cron_expression = db.Column(db.String(100))
    retention_policy = db.Column(db.Integer, nullable=False)
    notification_email = db.Column(db.String(200))
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    database_server = db.relationship('DatabaseServer', backref=db.backref('backup_jobs', lazy=True))
    storage_location = db.relationship('StorageLocation', backref=db.backref('backup_jobs', lazy=True))

class BackupHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    backup_job_id = db.Column(db.Integer, db.ForeignKey('backup_job.id'), nullable=False)
    start_time = db.Column(db.DateTime, nullable=False)
    end_time = db.Column(db.DateTime)
    status = db.Column(db.String(20), nullable=False)  # success, failed, running
    message = db.Column(db.Text)
    file_path = db.Column(db.String(500))
    file_size = db.Column(db.BigInteger)
    
    backup_job = db.relationship('BackupJob', backref=db.backref('history', lazy=True))