from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import enum

db = SQLAlchemy()


class DatabaseType(enum.Enum):
    """Database server types"""
    MYSQL = 'mysql'
    POSTGRES = 'postgres'


class StorageType(enum.Enum):
    """Storage location types"""
    LOCAL = 'local'
    FTP = 'ftp'
    S3 = 's3'
    BLOB = 'blob'


class DatabaseServer(db.Model):
    """Database server configuration"""
    __tablename__ = 'database_server'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    type = db.Column(db.Enum(DatabaseType), nullable=False)
    host = db.Column(db.String(200), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    username = db.Column(db.String(100), nullable=False)
    password = db.Column(db.String(200), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    
    # Relationship
    backup_jobs = db.relationship('BackupJob', backref='database_server', lazy=True)
    
    def test_connection(self):
        """Test connection to the database server"""
        try:
            if self.type == DatabaseType.MYSQL:
                import mysql.connector
                conn = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    user=self.username,
                    password=self.password,
                    connect_timeout=10
                )
                conn.close()
                return True, "Connection successful"
                
            elif self.type == DatabaseType.POSTGRES:
                import psycopg2
                conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.username,
                    password=self.password,
                    connect_timeout=10
                )
                conn.close()
                return True, "Connection successful"
                
            else:
                return False, f"Unsupported database type: {self.type}"
                
        except Exception as e:
            return False, str(e)
    
    def __repr__(self):
        return f"<DatabaseServer {self.name} ({self.type.value})>"


class StorageLocation(db.Model):
    """Storage location configuration"""
    __tablename__ = 'storage_location'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    type = db.Column(db.Enum(StorageType), nullable=False)
    config = db.Column(db.Text, nullable=False)  # JSON string of configuration
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    
    # Relationship
    backup_jobs = db.relationship('BackupJob', backref='storage_location', lazy=True)
    
    def get_config(self):
        """Get config as dictionary"""
        import json
        try:
            return json.loads(self.config) if self.config else {}
        except:
            return {}
    
    def __repr__(self):
        return f"<StorageLocation {self.name} ({self.type.value})>"


class BackupJob(db.Model):
    """Backup job configuration"""
    __tablename__ = 'backup_job'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    description = db.Column(db.Text)
    database_server_id = db.Column(db.Integer, db.ForeignKey('database_server.id'), nullable=False)
    databases = db.Column(db.Text, nullable=False)  # JSON string of selected databases
    storage_location_id = db.Column(db.Integer, db.ForeignKey('storage_location.id'), nullable=False)
    folder_path = db.Column(db.String(500), nullable=False)
    schedule_type = db.Column(db.String(20), nullable=False)  # daily, weekly, monthly, yearly
    cron_expression = db.Column(db.String(100))
    schedule_config = db.Column(db.Text)  # JSON string of schedule configuration
    retention_policy = db.Column(db.Integer, nullable=False, default=1)
    notification_email = db.Column(db.String(200))
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships - FIXED: Only ONE backref definition
    history = db.relationship('BackupHistory', backref='backup_job', lazy=True, cascade='all, delete-orphan')
    
    def get_databases_list(self):
        """Get databases as list"""
        import json
        try:
            return json.loads(self.databases) if self.databases else []
        except:
            return []
    
    def get_schedule_config_dict(self):
        """Get schedule config as dictionary"""
        import json
        try:
            return json.loads(self.schedule_config) if self.schedule_config else {}
        except:
            return {}
    
    def __repr__(self):
        return f"<BackupJob {self.name} (ID: {self.id})>"


class BackupHistory(db.Model):
    """Backup execution history"""
    __tablename__ = 'backup_history'
    
    id = db.Column(db.Integer, primary_key=True)
    backup_job_id = db.Column(db.Integer, db.ForeignKey('backup_job.id'), nullable=False)
    start_time = db.Column(db.DateTime, nullable=False)
    end_time = db.Column(db.DateTime)
    status = db.Column(db.String(20), nullable=False)  # success, failed, running, cancelled
    trigger_source = db.Column(db.String(20), default='manual')  # manual, daily, weekly, monthly, yearly
    message = db.Column(db.Text)
    file_paths = db.Column(db.Text)  # Semicolon-separated list of file paths
    log_path = db.Column(db.String(500))
    file_size = db.Column(db.BigInteger)
    is_cancelled = db.Column(db.Boolean, default=False)  # New field for cancellation
    cancelled_at = db.Column(db.DateTime)  # When the job was cancelled
    cancelled_by = db.Column(db.String(50))  # Who cancelled the job
    
    # NO backref here - it's defined in BackupJob
    
    def get_file_paths_list(self):
        """Get file paths as list"""
        if not self.file_paths:
            return []
        return [path.strip() for path in self.file_paths.split(';') if path.strip()]
    
    def get_duration_seconds(self):
        """Get duration in seconds"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def get_duration_formatted(self):
        """Get duration formatted as string"""
        duration = self.get_duration_seconds()
        if duration is None:
            return "N/A"
        
        if duration < 60:
            return f"{duration:.1f} seconds"
        elif duration < 3600:
            minutes = duration // 60
            seconds = duration % 60
            return f"{int(minutes)}m {int(seconds)}s"
        else:
            hours = duration // 3600
            minutes = (duration % 3600) // 60
            return f"{int(hours)}h {int(minutes)}m"
    
    def __repr__(self):
        return f"<BackupHistory {self.id} - Job {self.backup_job_id} - {self.status}>"


# Helper functions for database operations
def init_db():
    """Initialize the database - create all tables"""
    db.create_all()


def drop_db():
    """Drop all tables (use with caution!)"""
    db.drop_all()


def reset_db():
    """Reset the database (drop and recreate all tables)"""
    db.drop_all()
    db.create_all()


def get_backup_job_by_name(name):
    """Get a backup job by name"""
    return BackupJob.query.filter_by(name=name).first()


def get_active_backup_jobs():
    """Get all active backup jobs"""
    return BackupJob.query.filter_by(is_active=True).all()


def get_backup_history_for_job(job_id, limit=50):
    """Get backup history for a specific job"""
    return BackupHistory.query.filter_by(
        backup_job_id=job_id
    ).order_by(
        BackupHistory.start_time.desc()
    ).limit(limit).all()


def get_recent_backup_history(limit=100):
    """Get recent backup history across all jobs"""
    return BackupHistory.query.order_by(
        BackupHistory.start_time.desc()
    ).limit(limit).all()


def get_backup_statistics():
    """Get backup statistics"""
    from datetime import datetime, timedelta
    
    now = datetime.now()
    thirty_days_ago = now - timedelta(days=30)
    
    total_jobs = BackupJob.query.count()
    active_jobs = BackupJob.query.filter_by(is_active=True).count()
    
    successful = BackupHistory.query.filter(
        BackupHistory.status == 'success',
        BackupHistory.start_time >= thirty_days_ago
    ).count()
    
    failed = BackupHistory.query.filter(
        BackupHistory.status == 'failed',
        BackupHistory.start_time >= thirty_days_ago
    ).count()
    
    cancelled = BackupHistory.query.filter(
        BackupHistory.status == 'cancelled',
        BackupHistory.start_time >= thirty_days_ago
    ).count()
    
    running = BackupHistory.query.filter(
        BackupHistory.status == 'running',
        BackupHistory.start_time >= now - timedelta(hours=1)
    ).count()
    
    total_runs = successful + failed + cancelled
    success_rate = round((successful / total_runs * 100) if total_runs > 0 else 0, 1)
    
    return {
        'total_jobs': total_jobs,
        'active_jobs': active_jobs,
        'successful_runs': successful,
        'failed_runs': failed,
        'cancelled_runs': cancelled,
        'running_jobs': running,
        'success_rate': success_rate,
        'total_runs': total_runs
    }