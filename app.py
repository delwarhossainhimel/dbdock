from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, session, abort
from models import db, DatabaseServer, StorageLocation, BackupJob, BackupHistory, DatabaseType, StorageType
from werkzeug.security import check_password_hash, generate_password_hash
from dotenv import load_dotenv
from scheduler import init_scheduler, schedule_backup_job, unschedule_backup_job, enqueue_immediate_backup_job, send_test_email
from sqlalchemy import text, or_
from job_schedules import (
    build_schedule_config_from_form,
    get_cron_expression,
    get_primary_schedule_type,
    get_schedule_entries,
    normalize_schedule_config,
    validate_schedule_config,
)
from flask import send_file
from datetime import datetime
import json
import os
import fcntl
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

load_dotenv()

app = Flask(__name__)

# MySQL Configuration from Environment Variables
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = os.getenv('MYSQL_PORT', '3306')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'dbdock')
MYSQL_CHARSET = os.getenv('MYSQL_CHARSET', 'utf8mb4')

encoded_password = quote_plus(MYSQL_PASSWORD)
# Build MySQL connection string with encoded password
app.config['SQLALCHEMY_DATABASE_URI'] = (
    f"mysql+pymysql://{MYSQL_USER}:{encoded_password}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?charset={MYSQL_CHARSET}"
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')

# Example user storage (you can replace this with a DB)
users = {
    os.getenv('ADMIN_USER'): generate_password_hash(os.getenv('ADMIN_PASS'))
}

# Only check for multiple instances when running directly
if __name__ == '__main__' or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
    instance_lock_file = None
    try:
        instance_lock_file = open('/tmp/backup_app.lock', 'w')
        fcntl.flock(instance_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print("🔒 Application lock acquired")
    except (IOError, BlockingIOError):
        print("❌ Another instance of the application is already running")
        print("💡 Run: pkill -f python && rm -f /tmp/backup_*.lock")
        sys.exit(1)
else:
    instance_lock_file = None

db.init_app(app)

# Initialize scheduler after app is created
scheduler = None

# Create a custom filter for JSON parsing
@app.template_filter('from_json')
def from_json_filter(value):
    try:
        return json.loads(value)
    except:
        return {}

@app.template_filter('schedule_entries')
def schedule_entries_filter(value):
    return get_schedule_entries(value)

@app.template_filter('filesize')
def filesize_filter(size):
    if size is None:
        return "N/A"

    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024

@app.context_processor
def inject_log_helpers():
    def get_job_logs(job_id):
        logs_dir = Path(app.root_path) / 'backup_logs'
        if not logs_dir.exists():
            return []
        
        log_files = []
        for log_file in logs_dir.glob(f"backup_job_{job_id}_*.log"):
            try:
                schedule_type = log_file.name.replace(f"backup_job_{job_id}_", "").replace(".log", "")
                if schedule_type in ['daily', 'weekly', 'monthly', 'yearly']:
                    log_files.append({
                        'name': log_file.name,
                        'schedule_type': schedule_type,
                        'path': str(log_file),
                        'size': log_file.stat().st_size,
                        'modified': datetime.fromtimestamp(log_file.stat().st_mtime)
                    })
            except:
                pass
        log_files.sort(key=lambda x: x['modified'], reverse=True)
        return log_files
    
    return {
        'get_job_logs': get_job_logs,
        'view_schedule_log': view_schedule_log
    }

@app.context_processor
def inject_schedule_helpers():
    return {
        'get_schedule_entries': get_schedule_entries,
        'normalize_schedule_config': normalize_schedule_config,
    }

def ensure_database_schema():
    """Ensure MySQL schema has all required columns"""
    try:
        inspector = db.engine.execute(text("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'backup_job' 
            AND TABLE_SCHEMA = DATABASE()
        """))
        column_names = {row[0] for row in inspector}

        # Add missing columns for backup_job
        if 'schedule_config' not in column_names:
            db.session.execute(text("ALTER TABLE backup_job ADD COLUMN schedule_config TEXT"))
            print("✅ Added schedule_config column to backup_job")

        # Check backup_history columns
        inspector = db.engine.execute(text("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'backup_history' 
            AND TABLE_SCHEMA = DATABASE()
        """))
        history_column_names = {row[0] for row in inspector}

        if 'log_path' not in history_column_names:
            db.session.execute(text("ALTER TABLE backup_history ADD COLUMN log_path VARCHAR(500)"))
            print("✅ Added log_path column to backup_history")

        db.session.commit()
        print("✅ Database schema check completed")
    except Exception as e:
        print(f"⚠️ Error checking database schema: {e}")
        db.session.rollback()

def migrate_backup_job_schedules():
    jobs = BackupJob.query.all()
    updated = False

    for job in jobs:
        normalized = normalize_schedule_config(
            job.schedule_config,
            job.schedule_type,
            job.cron_expression,
            fallback_retention=job.retention_policy,
        )
        primary_schedule_type = get_primary_schedule_type(normalized)
        primary_entry = next((entry for entry in get_schedule_entries(normalized) if entry['type'] == primary_schedule_type), None)

        normalized_json = json.dumps(normalized)
        if job.schedule_config != normalized_json:
            job.schedule_config = normalized_json
            updated = True

        if primary_schedule_type and job.schedule_type != primary_schedule_type:
            job.schedule_type = primary_schedule_type
            updated = True

        if primary_entry and job.cron_expression != primary_entry['cron_expression']:
            job.cron_expression = primary_entry['cron_expression']
            updated = True

        if primary_schedule_type:
            primary_retention = int(normalized[primary_schedule_type].get('retention', job.retention_policy or 1))
            if job.retention_policy != primary_retention:
                job.retention_policy = primary_retention
                updated = True

    if updated:
        db.session.commit()
        print("✅ Migrated backup job schedules")

def add_unique_constraint_to_job_name():
    """Add unique constraint to backup_job name if it doesn't exist"""
    try:
        # Check if the constraint already exists
        result = db.session.execute(text("""
            SELECT CONSTRAINT_NAME 
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
            WHERE TABLE_NAME = 'backup_job' 
            AND CONSTRAINT_TYPE = 'UNIQUE'
            AND TABLE_SCHEMA = DATABASE()
        """))
        
        constraints = [row[0] for row in result]
        
        # If no unique constraint exists, add it
        if not any('name' in constraint.lower() for constraint in constraints):
            # First, check for duplicate names
            duplicates = db.session.execute(text("""
                SELECT name, COUNT(*) as count 
                FROM backup_job 
                GROUP BY name 
                HAVING COUNT(*) > 1
            """)).fetchall()
            
            if duplicates:
                print("⚠️ Found duplicate job names. Please resolve them manually:")
                for dup in duplicates:
                    print(f"   - {dup[0]} appears {dup[1]} times")
                print("💡 You can rename duplicates before adding unique constraint")
                return
            
            # Add unique constraint
            db.session.execute(text("""
                ALTER TABLE backup_job 
                ADD CONSTRAINT unique_job_name UNIQUE (name)
            """))
            db.session.commit()
            print("✅ Added unique constraint to backup_job.name")
    except Exception as e:
        print(f"⚠️ Error adding unique constraint: {e}")
        db.session.rollback()

def test_mysql_connection():
    """Test MySQL connection on startup"""
    try:
        # Test connection by executing a simple query
        result = db.session.execute(text("SELECT 1")).fetchone()
        print(f"✅ MySQL connection successful! Connected to {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
        return True
    except Exception as e:
        print(f"❌ MySQL connection failed: {e}")
        print(f"   Host: {MYSQL_HOST}")
        print(f"   Port: {MYSQL_PORT}")
        print(f"   Database: {MYSQL_DATABASE}")
        print(f"   User: {MYSQL_USER}")
        return False

# Initialize the application
with app.app_context():
    # Test MySQL connection first
    test_mysql_connection()
    
    # Create tables if they don't exist
    db.create_all()
    ensure_database_schema()
    add_unique_constraint_to_job_name()
    migrate_backup_job_schedules()
    
    print("🔄 Initializing scheduler...")
    
    # Initialize scheduler
    scheduler = init_scheduler(app)
    
    if scheduler:
        from scheduler import get_scheduler_status
        status = get_scheduler_status()
        
        if status['running']:
            print("✅ Scheduler is RUNNING and ready")
        else:
            print("❌ Scheduler initialized but NOT running")
        
        # Schedule existing jobs on startup
        jobs = BackupJob.query.filter_by(is_active=True).all()
        print(f"📋 Found {len(jobs)} active jobs to schedule")
        
        scheduled_count = 0
        for job in jobs:
            try:
                schedule_backup_job(scheduler, job)
                scheduled_count += 1
            except Exception as e:
                print(f"❌ Failed to schedule job {job.name}: {e}")
        
        print(f"✅ Successfully scheduled {scheduled_count}/{len(jobs)} jobs")
        
    else:
        print("❌ Scheduler failed to initialize completely")

@app.route('/health', methods=['GET'])
def health_check():
    # Check database connection
    try:
        db.session.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return jsonify({
        "status": "healthy",
        "message": "Application is running fine",
        "database": {
            "host": MYSQL_HOST,
            "port": MYSQL_PORT,
            "database": MYSQL_DATABASE,
            "status": db_status
        }
    }), 200

@app.before_request
def require_login():
    # Allow login and static files
    allowed_routes = ['login', 'static', 'health_check']
    if 'user' not in session and request.endpoint not in allowed_routes:
        return redirect(url_for('login'))
    
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        if username in users and check_password_hash(users[username], password):
            session['user'] = username
            flash("Login successful!", "success")
            return redirect(url_for('dashboard'))
        else:
            flash("Invalid username or password", "danger")
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('user', None)
    flash("You have been logged out.", "info")
    return redirect(url_for('login'))

@app.route('/')
def dashboard():
    if 'user' not in session:
        return redirect(url_for('login'))
    
    # Get basic counts
    servers = DatabaseServer.query.filter_by(is_active=True).count()
    locations = StorageLocation.query.filter_by(is_active=True).count()
    total_jobs = BackupJob.query.count()
    active_jobs = BackupJob.query.filter_by(is_active=True).count()
    
    # Get job execution statistics (last 30 days)
    from datetime import datetime, timedelta
    thirty_days_ago = datetime.now() - timedelta(days=30)
    
    successful_runs = BackupHistory.query.filter(
        BackupHistory.status == 'success',
        BackupHistory.start_time >= thirty_days_ago
    ).count()
    
    failed_runs = BackupHistory.query.filter(
        BackupHistory.status == 'failed',
        BackupHistory.start_time >= thirty_days_ago
    ).count()
    
    # Get currently running jobs
    running_jobs = BackupHistory.query.filter(
        BackupHistory.status == 'running',
        BackupHistory.start_time >= datetime.now() - timedelta(hours=1)
    ).count()
    
    # Calculate success rate
    total_runs = successful_runs + failed_runs
    if total_runs > 0:
        success_rate = round((successful_runs / total_runs) * 100, 1)
    else:
        success_rate = 0
    
    # Get recent history
    recent_history = BackupHistory.query.order_by(
        BackupHistory.start_time.desc()
    ).limit(10).all()
    
    return render_template('dashboard.html', 
                          servers=servers, 
                          locations=locations, 
                          jobs=active_jobs,
                          total_jobs=total_jobs,
                          active_jobs=active_jobs,
                          successful_runs=successful_runs,
                          failed_runs=failed_runs,
                          running_jobs=running_jobs,
                          success_rate=success_rate,
                          history=recent_history)


# ============================================
# DATABASE SERVERS ROUTES
# ============================================

@app.route('/database_servers', methods=['GET', 'POST'])
def database_servers():
    if request.method == 'POST':
        name = request.form.get('name')
        db_type = request.form.get('type')
        host = request.form.get('host')
        port = request.form.get('port')
        username = request.form.get('username')
        password = request.form.get('password')
        
        server = DatabaseServer(
            name=name,
            type=DatabaseType(db_type),
            host=host,
            port=port,
            username=username,
            password=password
        )
        
        db.session.add(server)
        db.session.commit()
        
        return redirect(url_for('database_servers'))
    
    servers = DatabaseServer.query.all()
    return render_template('database_servers.html', servers=servers)

@app.route('/delete_server/<int:server_id>', methods=['POST'])
def delete_server(server_id):
    server = DatabaseServer.query.get_or_404(server_id)
    
    # Check if any backup jobs are using this server
    jobs_using_server = BackupJob.query.filter_by(database_server_id=server_id).count()
    
    if jobs_using_server > 0:
        return jsonify({
            'success': False, 
            'message': f'Cannot delete server. It is used by {jobs_using_server} backup job(s).'
        })
    
    db.session.delete(server)
    db.session.commit()
    
    return jsonify({'success': True, 'message': 'Server deleted successfully'})

@app.route('/edit_server/<int:server_id>', methods=['GET'])
def edit_server(server_id):
    try:
        print(f"Attempting to edit server with ID: {server_id}")
        server = DatabaseServer.query.get_or_404(server_id)
        print(f"Found server: {server.name}")
        return render_template('edit_server.html', server=server)
    except Exception as e:
        print(f"Error in edit_server: {str(e)}")
        return f"Error: {str(e)}", 500

@app.route('/edit_server/<int:server_id>', methods=['POST'])
def update_server(server_id):
    server = DatabaseServer.query.get_or_404(server_id)
    
    server.name = request.form.get('name')
    server.type = DatabaseType(request.form.get('type'))
    server.host = request.form.get('host')
    server.port = request.form.get('port')
    server.username = request.form.get('username')
    
    # Only update password if provided
    if request.form.get('password'):
        server.password = request.form.get('password')
    
    db.session.commit()
    
    return redirect(url_for('database_servers'))

@app.route('/test_connection/<int:server_id>')
def test_connection(server_id):
    server = DatabaseServer.query.get_or_404(server_id)
    success, message = server.test_connection()
    return jsonify({'success': success, 'message': message})

# ============================================
# STORAGE LOCATIONS ROUTES
# ============================================

@app.route('/storage_locations', methods=['GET', 'POST'])
def storage_locations():
    if request.method == 'POST':
        name = request.form.get('name')
        storage_type = request.form.get('type')
        
        # Get configuration based on storage type
        config = {}
        if storage_type == 'local':
            config['path'] = request.form.get('local_path')
        elif storage_type == 'ftp':
            config['host'] = request.form.get('ftp_host')
            config['port'] = request.form.get('ftp_port')
            config['username'] = request.form.get('ftp_username')
            config['password'] = request.form.get('ftp_password')
            config['passive_mode'] = request.form.get('ftp_passive', 'true') == 'true'
        elif storage_type == 's3':
            config['bucket'] = request.form.get('s3_bucket')
            config['access_key'] = request.form.get('s3_access_key')
            config['secret_key'] = request.form.get('s3_secret_key')
            config['region'] = request.form.get('s3_region')
        elif storage_type == 'blob':
            config['connection_string'] = request.form.get('blob_connection_string')
            config['container'] = request.form.get('blob_container')
        
        location = StorageLocation(
            name=name,
            type=StorageType(storage_type),
            config=json.dumps(config)
        )
        
        db.session.add(location)
        db.session.commit()
        
        return redirect(url_for('storage_locations'))
    
    locations = StorageLocation.query.all()
    return render_template('storage_locations.html', locations=locations)

@app.route('/edit_location/<int:location_id>', methods=['GET'])
def edit_location(location_id):
    try:
        print(f"Attempting to edit storage location with ID: {location_id}")
        location = StorageLocation.query.get_or_404(location_id)
        print(f"Found storage location: {location.name}")
        
        # Parse the config JSON
        config = json.loads(location.config) if location.config else {}
        return render_template('edit_location.html', location=location, config=config)
    except Exception as e:
        print(f"Error in edit_location: {str(e)}")
        return f"Error: {str(e)}", 500

@app.route('/edit_location/<int:location_id>', methods=['POST'])
def update_location(location_id):
    location = StorageLocation.query.get_or_404(location_id)
    
    location.name = request.form.get('name')
    storage_type = request.form.get('type')
    location.type = StorageType(storage_type)
    
    # Get configuration based on storage type
    config = {}
    if storage_type == 'local':
        config['path'] = request.form.get('local_path')
    elif storage_type == 'ftp':
        config['host'] = request.form.get('ftp_host')
        config['port'] = request.form.get('ftp_port')
        config['username'] = request.form.get('ftp_username')
        config['password'] = request.form.get('ftp_password')
        config['passive_mode'] = request.form.get('ftp_passive', 'true') == 'true'
    elif storage_type == 's3':
        config['bucket'] = request.form.get('s3_bucket')
        config['access_key'] = request.form.get('s3_access_key')
        config['secret_key'] = request.form.get('s3_secret_key')
        config['region'] = request.form.get('s3_region')
    elif storage_type == 'blob':
        config['connection_string'] = request.form.get('blob_connection_string')
        config['container'] = request.form.get('blob_container')
    
    location.config = json.dumps(config)
    db.session.commit()
    
    return redirect(url_for('storage_locations'))

@app.route('/delete_location/<int:location_id>', methods=['POST'])
def delete_location(location_id):
    location = StorageLocation.query.get_or_404(location_id)
    
    # Check if any backup jobs are using this location
    jobs_using_location = BackupJob.query.filter_by(storage_location_id=location_id).count()
    
    if jobs_using_location > 0:
        return jsonify({
            'success': False, 
            'message': f'Cannot delete storage location. It is used by {jobs_using_location} backup job(s).'
        })
    
    db.session.delete(location)
    db.session.commit()
    
    return jsonify({'success': True, 'message': 'Storage location deleted successfully'})

# ============================================
# BACKUP JOBS ROUTES
# ============================================

@app.route('/backup_jobs', methods=['GET', 'POST'])
def backup_jobs():
    if request.method == 'POST':
        name = request.form.get('name')
        description = request.form.get('description')
        database_server_id = request.form.get('database_server')
        databases = request.form.getlist('databases')
        storage_location_id = request.form.get('storage_location')
        folder_path = request.form.get('folder_path')
        schedule_config = build_schedule_config_from_form(request.form)
        notification_email = request.form.get('notification_email')

        # Check if job name already exists
        existing_job = BackupJob.query.filter_by(name=name).first()
        if existing_job:
            flash(f'A backup job with the name "{name}" already exists. Please choose a different name.', "danger")
            
            # Get all data needed for the template
            servers = DatabaseServer.query.filter_by(is_active=True).all()
            locations = StorageLocation.query.filter_by(is_active=True).all()
            jobs = BackupJob.query.order_by(BackupJob.name.asc()).all()
            
            # Get latest history for each job
            latest_history_by_job = {}
            for job in jobs:
                job.normalized_schedule_config = normalize_schedule_config(
                    job.schedule_config,
                    job.schedule_type,
                    job.cron_expression,
                    fallback_retention=job.retention_policy,
                )
            for record in BackupHistory.query.order_by(
                BackupHistory.backup_job_id.asc(),
                BackupHistory.start_time.desc(),
                BackupHistory.id.desc(),
            ).all():
                latest_history_by_job.setdefault(record.backup_job_id, record)
            for job in jobs:
                job.latest_history = latest_history_by_job.get(job.id)
            
            # Prepare form data to re-populate the modal
            form_data = {
                'name': name,
                'description': description,
                'database_server_id': database_server_id,
                'databases': databases,
                'storage_location_id': storage_location_id,
                'folder_path': folder_path,
                'notification_email': notification_email,
                'schedule_types': request.form.getlist('schedule_types'),
                'daily_time': request.form.get('daily_time'),
                'daily_retention': request.form.get('daily_retention'),
                'weekly_day': request.form.get('weekly_day'),
                'weekly_time': request.form.get('weekly_time'),
                'weekly_retention': request.form.get('weekly_retention'),
                'monthly_day': request.form.get('monthly_day'),
                'monthly_time': request.form.get('monthly_time'),
                'monthly_retention': request.form.get('monthly_retention'),
                'yearly_month': request.form.get('yearly_month'),
                'yearly_day': request.form.get('yearly_day'),
                'yearly_time': request.form.get('yearly_time'),
                'yearly_retention': request.form.get('yearly_retention'),
            }
            
            # Pass form data and error flag to template
            return render_template('backup_jobs.html', 
                                  servers=servers, 
                                  locations=locations, 
                                  jobs=jobs,
                                  error=True,
                                  form_data=form_data)

        is_valid, validation_message = validate_schedule_config(schedule_config)
        if not is_valid:
            flash(validation_message, "danger")
            return redirect(url_for('backup_jobs'))

        primary_schedule_type = get_primary_schedule_type(schedule_config)
        cron_expression = get_cron_expression(primary_schedule_type, schedule_config[primary_schedule_type])
        
        # Handle "All Databases" - store as special value
        if '__all__' in databases:
            databases = ['__all__']  # Store as '__all__' to indicate auto-detect
        
        try:
            job = BackupJob(
                name=name,
                description=description,
                database_server_id=database_server_id,
                databases=json.dumps(databases),
                storage_location_id=storage_location_id,
                folder_path=folder_path,
                schedule_type=primary_schedule_type,
                cron_expression=cron_expression,
                schedule_config=json.dumps(schedule_config),
                retention_policy=int(schedule_config[primary_schedule_type]['retention']),
                notification_email=notification_email
            )
            
            db.session.add(job)
            db.session.commit()
            
            # Schedule the job
            schedule_backup_job(scheduler, job)
            
            flash(f'Backup job "{name}" created successfully!', "success")
            return redirect(url_for('backup_jobs'))
            
        except Exception as e:
            db.session.rollback()
            flash(f'Error creating job: {str(e)}', "danger")
            return redirect(url_for('backup_jobs'))
    
    # GET request - existing code with pagination
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    search = request.args.get('search', '', type=str)
    status_filter = request.args.get('status', 'all', type=str)
    
    # Build query
    query = BackupJob.query
    
    # Apply search filter
    if search:
        search_term = f"%{search}%"
        query = query.join(DatabaseServer).join(StorageLocation).filter(
            or_(
                BackupJob.name.ilike(search_term),
                DatabaseServer.name.ilike(search_term),
                StorageLocation.name.ilike(search_term)
            )
        )
    
    # Apply status filter
    if status_filter == 'active':
        query = query.filter_by(is_active=True)
    elif status_filter == 'inactive':
        query = query.filter_by(is_active=False)
    
    # Order by name
    query = query.order_by(BackupJob.name.asc())
    
    # Paginate
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    jobs = pagination.items
    
    # Get all servers and locations for the modal
    servers = DatabaseServer.query.filter_by(is_active=True).all()
    locations = StorageLocation.query.filter_by(is_active=True).all()
    
    # Get latest history for each job
    latest_history_by_job = {}
    job_ids = [job.id for job in jobs]
    if job_ids:
        history_records = BackupHistory.query.filter(
            BackupHistory.backup_job_id.in_(job_ids)
        ).order_by(
            BackupHistory.backup_job_id.asc(),
            BackupHistory.start_time.desc(),
            BackupHistory.id.desc(),
        ).all()
        
        for record in history_records:
            if record.backup_job_id not in latest_history_by_job:
                latest_history_by_job[record.backup_job_id] = record
    
    # Add normalized schedule config and latest history to each job
    for job in jobs:
        job.normalized_schedule_config = normalize_schedule_config(
            job.schedule_config,
            job.schedule_type,
            job.cron_expression,
            fallback_retention=job.retention_policy,
        )
        job.latest_history = latest_history_by_job.get(job.id)
    
    return render_template('backup_jobs.html', 
                          jobs=jobs,
                          servers=servers,
                          locations=locations,
                          pagination=pagination,
                          search=search,
                          status_filter=status_filter,
                          per_page=per_page)

@app.route('/delete_job/<int:job_id>', methods=['POST'])
def delete_job(job_id):
    job = BackupJob.query.get_or_404(job_id)
    
    try:
        # Remove the job from scheduler
        unschedule_backup_job(scheduler, job_id)
        
        # First, delete all backup history records for this job
        # This prevents the foreign key constraint error
        BackupHistory.query.filter_by(backup_job_id=job_id).delete()
        
        # Then delete the job
        db.session.delete(job)
        db.session.commit()
        
        return jsonify({'success': True, 'message': 'Backup job deleted successfully'})
    except Exception as e:
        db.session.rollback()
        app.logger.error(f"Error deleting job {job_id}: {str(e)}")
        return jsonify({'success': False, 'message': f'Error deleting job: {str(e)}'}), 500

@app.route('/edit_job/<int:job_id>', methods=['GET'])
def edit_job(job_id):
    job = BackupJob.query.get_or_404(job_id)
    servers = DatabaseServer.query.filter_by(is_active=True).all()
    locations = StorageLocation.query.filter_by(is_active=True).all()
    
    # Parse the databases from JSON
    databases = json.loads(job.databases) if job.databases else []
    
    job.normalized_schedule_config = normalize_schedule_config(
        job.schedule_config,
        job.schedule_type,
        job.cron_expression,
        fallback_retention=job.retention_policy,
    )

    return render_template('edit_job.html', 
                         job=job, 
                         servers=servers, 
                         locations=locations,
                         selected_databases=databases)

@app.route('/edit_job/<int:job_id>', methods=['POST'])
def update_job(job_id):
    job = BackupJob.query.get_or_404(job_id)
    
    new_name = request.form.get('name')
    
    # Check if the new name conflicts with another job
    if new_name != job.name:
        existing_job = BackupJob.query.filter_by(name=new_name).first()
        if existing_job:
            flash(f'A backup job with the name "{new_name}" already exists. Please choose a different name.', "danger")
            return redirect(url_for('edit_job', job_id=job_id))
    
    job.name = new_name
    job.description = request.form.get('description')
    job.database_server_id = request.form.get('database_server')
    
    databases = request.form.getlist('databases')
    if '__all__' in databases:
        databases = ['__all__']
    job.databases = json.dumps(databases)
    
    job.storage_location_id = request.form.get('storage_location')
    job.folder_path = request.form.get('folder_path')
    job.notification_email = request.form.get('notification_email')

    schedule_config = build_schedule_config_from_form(request.form)
    is_valid, validation_message = validate_schedule_config(schedule_config)
    if not is_valid:
        flash(validation_message, "danger")
        return redirect(url_for('edit_job', job_id=job.id))

    job.schedule_type = get_primary_schedule_type(schedule_config)
    job.cron_expression = get_cron_expression(job.schedule_type, schedule_config[job.schedule_type])
    job.schedule_config = json.dumps(schedule_config)
    job.retention_policy = int(schedule_config[job.schedule_type]['retention'])
    
    try:
        db.session.commit()
        # Reschedule the job
        schedule_backup_job(scheduler, job)
        flash(f'Backup job "{job.name}" updated successfully!', "success")
    except Exception as e:
        db.session.rollback()
        flash(f'Error updating job: {str(e)}', "danger")
    
    return redirect(url_for('backup_jobs'))

@app.route('/run_job/<int:job_id>', methods=['POST'])
def run_job(job_id):
    job = BackupJob.query.get_or_404(job_id)

    if not enqueue_immediate_backup_job(scheduler, job.id):
        return jsonify({'success': False, 'message': 'Scheduler is not running, so the job could not be queued.'}), 503

    return jsonify({'success': True, 'message': 'Backup job queued successfully and is running in the background.'})

@app.route('/toggle_job/<int:job_id>', methods=['POST'])
def toggle_job(job_id):
    job = BackupJob.query.get_or_404(job_id)
    job.is_active = not job.is_active
    db.session.commit()
    
    if job.is_active:
        schedule_backup_job(scheduler, job)
        message = 'Job enabled successfully'
    else:
        unschedule_backup_job(scheduler, job_id)
        message = 'Job disabled successfully'
    
    return jsonify({'success': True, 'message': message, 'is_active': job.is_active})

# ============================================
# EMAIL ROUTES
# ============================================

@app.route('/test_email', methods=['POST'])
def test_email():
    payload = request.get_json(silent=True) or {}
    recipient_email = (payload.get('email') or '').strip()

    if not recipient_email:
        return jsonify({'success': False, 'message': 'Enter a recipient email first.'}), 400

    try:
        send_test_email(recipient_email)
        return jsonify({'success': True, 'message': f'Test email sent to {recipient_email}.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 400

# ============================================
# API ROUTES
# ============================================

@app.route('/api/jobs')
def api_get_jobs():
    """API endpoint for getting jobs with pagination and filtering"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 2, type=int)
    search = request.args.get('search', '', type=str)
    status = request.args.get('status', 'all', type=str)
    
    query = BackupJob.query
    
    if search:
        search_term = f"%{search}%"
        query = query.join(DatabaseServer).join(StorageLocation).filter(
            or_(
                BackupJob.name.ilike(search_term),
                DatabaseServer.name.ilike(search_term),
                StorageLocation.name.ilike(search_term)
            )
        )
    
    if status == 'active':
        query = query.filter_by(is_active=True)
    elif status == 'inactive':
        query = query.filter_by(is_active=False)
    
    query = query.order_by(BackupJob.name.asc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    
    jobs = []
    for job in pagination.items:
        databases = json.loads(job.databases) if job.databases else []
        jobs.append({
            'id': job.id,
            'name': job.name,
            'description': job.description,
            'is_active': job.is_active,
            'databases': databases,
            'auto_detect': '__all__' in databases,
            'database_server': {
                'id': job.database_server.id,
                'name': job.database_server.name,
                'type': job.database_server.type.value
            },
            'storage_location': {
                'id': job.storage_location.id,
                'name': job.storage_location.name,
                'type': job.storage_location.type.value
            },
            'folder_path': job.folder_path,
            'schedule_type': job.schedule_type,
            'cron_expression': job.cron_expression,
            'retention_policy': job.retention_policy,
            'notification_email': job.notification_email,
            'created_at': job.created_at.isoformat() if job.created_at else None,
            'updated_at': job.updated_at.isoformat() if job.updated_at else None,
        })
    
    return jsonify({
        'jobs': jobs,
        'total': pagination.total,
        'page': pagination.page,
        'per_page': pagination.per_page,
        'pages': pagination.pages,
        'has_prev': pagination.has_prev,
        'has_next': pagination.has_next,
        'prev_num': pagination.prev_num,
        'next_num': pagination.next_num,
    })

@app.route('/api/check-job-name', methods=['POST'])
def check_job_name():
    """Check if a job name already exists"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'exists': False, 'error': 'No data provided'}), 400
            
        name = data.get('name', '').strip()
        exclude_id = data.get('exclude_id')
        
        if not name:
            return jsonify({'exists': False, 'error': 'Name is required'}), 400
        
        query = BackupJob.query.filter(db.func.lower(BackupJob.name) == db.func.lower(name))
        if exclude_id:
            query = query.filter(BackupJob.id != exclude_id)
        
        exists = query.first() is not None
        return jsonify({'exists': exists})
    except Exception as e:
        return jsonify({'exists': False, 'error': str(e)}), 500

@app.route('/get_databases/<int:server_id>')
def get_databases(server_id):
    server = DatabaseServer.query.get_or_404(server_id)
    
    try:
        if server.type == DatabaseType.MYSQL:
            import mysql.connector
            conn = mysql.connector.connect(
                host=server.host,
                port=server.port,
                user=server.username,
                password=server.password
            )
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in cursor.fetchall() if db[0] not in ['information_schema', 'mysql', 'performance_schema', 'sys']]
            cursor.close()
            conn.close()
        elif server.type == DatabaseType.POSTGRES:
            import psycopg2
            conn = psycopg2.connect(
                host=server.host,
                port=server.port,
                user=server.username,
                password=server.password,
                database='postgres'
            )
            cursor = conn.cursor()
            cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
            databases = [db[0] for db in cursor.fetchall() if db[0] != 'postgres']
            cursor.close()
            conn.close()
        else:
            return jsonify({'success': False, 'message': 'Unsupported database type'})
        
        return jsonify({'success': True, 'databases': databases})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# ============================================
# REPORTS ROUTES
# ============================================

@app.route('/reports')
def reports():
    job_id = request.args.get('job_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    page = request.args.get('page', 1, type=int)
    
    query = BackupHistory.query
    
    if job_id:
        query = query.filter_by(backup_job_id=job_id)
    
    if start_date:
        query = query.filter(BackupHistory.start_time >= datetime.strptime(start_date, '%Y-%m-%d'))
    
    if end_date:
        query = query.filter(BackupHistory.start_time <= datetime.strptime(end_date, '%Y-%m-%d'))
    
    pagination = query.order_by(BackupHistory.start_time.desc()).paginate(page=page, per_page=20, error_out=False)
    history = pagination.items
    jobs = BackupJob.query.all()
    
    return render_template('reports.html', history=history, jobs=jobs, pagination=pagination)

# @app.route('/reports/<int:history_id>/log')
# def view_backup_log(history_id):
#     record = BackupHistory.query.get_or_404(history_id)

#     if not record.log_path:
#         flash("No log file is available for this backup run.", "warning")
#         return redirect(url_for('reports'))

#     log_path = Path(record.log_path).resolve()
#     logs_root = (Path(app.root_path) / 'backup_logs').resolve()

#     try:
#         log_path.relative_to(logs_root)
#     except ValueError:
#         abort(404)

#     if not log_path.exists() or not log_path.is_file():
#         flash("The log file could not be found on disk.", "warning")
#         return redirect(url_for('reports'))

#     log_content = log_path.read_text(encoding='utf-8', errors='replace')
#     return render_template('backup_log.html', record=record, log_content=log_content)
@app.route('/reports/<int:history_id>/log')
def view_backup_log(history_id):
    record = BackupHistory.query.get_or_404(history_id)

    if not record.log_path:
        flash("No log file is available for this backup run.", "warning")
        return redirect(url_for('reports'))

    log_path = Path(record.log_path).resolve()
    logs_root = (Path(app.root_path) / 'backup_logs').resolve()

    try:
        log_path.relative_to(logs_root)
    except ValueError:
        abort(404)

    # If the specific log doesn't exist, try to find it
    if not log_path.exists() or not log_path.is_file():
        # Try to find the log file with schedule-specific naming
        job_id = record.backup_job_id
        trigger_source = getattr(record, 'trigger_source', 'daily')
        
        # Check if the log file exists with schedule-specific name
        schedule_log_path = logs_root / f"backup_job_{job_id}_{trigger_source}.log"
        if schedule_log_path.exists():
            log_path = schedule_log_path
        else:
            flash("The log file could not be found on disk.", "warning")
            return redirect(url_for('reports'))

    log_content = log_path.read_text(encoding='utf-8', errors='replace')
    return render_template('backup_log.html', record=record, log_content=log_content)

# @app.route('/logs/job/<int:job_id>')
# def view_job_logs(job_id):
#     """View all logs for a specific job"""
#     job = BackupJob.query.get_or_404(job_id)
#     logs_dir = Path(app.root_path) / 'backup_logs'
    
#     if not logs_dir.exists():
#         flash("No logs found for this job.", "warning")
#         return redirect(url_for('backup_jobs'))
    
#     # Get all log files for this job
#     log_files = []
#     for log_file in logs_dir.glob(f"backup_job_{job_id}_*.log"):
#         try:
#             # Extract schedule type from filename
#             schedule_type = log_file.name.replace(f"backup_job_{job_id}_", "").replace(".log", "")
#             if schedule_type in ['daily', 'weekly', 'monthly', 'yearly']:
#                 log_files.append({
#                     'name': log_file.name,
#                     'schedule_type': schedule_type,
#                     'path': str(log_file),
#                     'size': log_file.stat().st_size,
#                     'modified': datetime.fromtimestamp(log_file.stat().st_mtime)
#                 })
#         except Exception as e:
#             print(f"Error processing log file {log_file}: {e}")
    
#     # Sort by modified time (newest first)
#     log_files.sort(key=lambda x: x['modified'], reverse=True)
    
#     return render_template('job_logs.html', job=job, log_files=log_files)

@app.route('/logs/job/<int:job_id>')
def view_job_logs(job_id):
    """View all logs for a specific job"""
    job = BackupJob.query.get_or_404(job_id)
    logs_dir = Path(app.root_path) / 'backup_logs'
    
    if not logs_dir.exists():
        flash("No logs found for this job.", "warning")
        return redirect(url_for('backup_jobs'))
    
    # Get all log files for this job
    log_files = []
    for log_file in logs_dir.glob(f"backup_job_{job_id}_*.log"):
        try:
            # Extract schedule type from filename
            schedule_type = log_file.name.replace(f"backup_job_{job_id}_", "").replace(".log", "")
            if schedule_type in ['daily', 'weekly', 'monthly', 'yearly']:
                log_files.append({
                    'name': log_file.name,
                    'schedule_type': schedule_type,
                    'path': str(log_file),
                    'size': log_file.stat().st_size,
                    'modified': datetime.fromtimestamp(log_file.stat().st_mtime)
                })
        except Exception as e:
            print(f"Error processing log file {log_file}: {e}")
    
    # Sort by modified time (newest first)
    log_files.sort(key=lambda x: x['modified'], reverse=True)
    
    return render_template('job_logs.html', job=job, log_files=log_files)

# @app.route('/logs/job/<int:job_id>/<schedule_type>')
# def view_schedule_log(job_id, schedule_type):
#     """View a specific schedule log"""
#     job = BackupJob.query.get_or_404(job_id)
#     logs_dir = Path(app.root_path) / 'backup_logs'
#     log_path = logs_dir / f"backup_job_{job_id}_{schedule_type}.log"
    
#     if not log_path.exists():
#         flash(f"No log file found for {schedule_type} schedule.", "warning")
#         return redirect(url_for('view_job_logs', job_id=job_id))
    
#     log_content = log_path.read_text(encoding='utf-8', errors='replace')
    
#     # Create a fake history record for display
#     class FakeHistory:
#         def __init__(self, job, schedule_type):
#             self.backup_job = job
#             self.backup_job_id = job.id
#             self.trigger_source = schedule_type
#             self.start_time = datetime.fromtimestamp(log_path.stat().st_mtime)
#             self.end_time = self.start_time
#             self.status = 'success'  # Default, will be determined from content
#             self.log_path = str(log_path)
#             self.file_size = log_path.stat().st_size
#             self.message = f"Showing {schedule_type} schedule log"
    
#     # Try to determine status from log content
#     status = 'success'
#     if 'FAILED' in log_content or 'Error' in log_content:
#         status = 'failed'
#     elif 'Running' in log_content or 'running' in log_content:
#         status = 'running'
    
#     fake_record = FakeHistory(job, schedule_type)
#     fake_record.status = status
    
#     return render_template('backup_log.html', record=fake_record, log_content=log_content)

@app.route('/logs/job/<int:job_id>/<schedule_type>')
def view_schedule_log(job_id, schedule_type):
    """View a specific schedule log"""
    from datetime import datetime
    from pathlib import Path
    
    job = BackupJob.query.get_or_404(job_id)
    logs_dir = Path(app.root_path) / 'backup_logs'
    log_path = logs_dir / f"backup_job_{job_id}_{schedule_type}.log"
    
    if not log_path.exists():
        flash(f"No log file found for {schedule_type} schedule.", "warning")
        return redirect(url_for('view_job_logs', job_id=job_id))
    
    log_content = log_path.read_text(encoding='utf-8', errors='replace')
    
    # Create a proper class for the fake history with all required attributes
    class FakeHistory:
        def __init__(self, job, schedule_type, log_path):
            # Add all attributes that might be needed by the template
            self.id = 9999  # Use a large number to indicate it's a fake record
            self.backup_job = job
            self.backup_job_id = job.id
            self.trigger_source = schedule_type
            self.start_time = datetime.fromtimestamp(log_path.stat().st_mtime)
            self.end_time = self.start_time
            self.status = 'success'  # Default
            self.log_path = str(log_path)
            self.file_size = log_path.stat().st_size
            self.message = f"Showing {schedule_type} schedule log"
            self.file_path = None  # Add this attribute if needed
    
    # Try to determine status from log content
    status = 'success'
    log_content_lower = log_content.lower()
    if 'failed' in log_content_lower or 'error' in log_content_lower:
        status = 'failed'
    elif 'running' in log_content_lower:
        status = 'running'
    
    fake_record = FakeHistory(job, schedule_type, log_path)
    fake_record.status = status
    
    return render_template('backup_log.html', record=fake_record, log_content=log_content)

# @app.route('/logs/download/<int:history_id>')
# def download_log(history_id):
#     """Download the log file"""
#     record = BackupHistory.query.get_or_404(history_id)
    
#     if not record.log_path:
#         flash("No log file available for download.", "warning")
#         return redirect(url_for('reports'))
    
#     log_path = Path(record.log_path).resolve()
#     logs_root = (Path(app.root_path) / 'backup_logs').resolve()
    
#     try:
#         log_path.relative_to(logs_root)
#     except ValueError:
#         abort(404)
    
#     if not log_path.exists() or not log_path.is_file():
#         flash("The log file could not be found.", "warning")
#         return redirect(url_for('reports'))
    
#     return send_file(log_path, as_attachment=True, download_name=log_path.name)

@app.route('/logs/download/<int:history_id>')
def download_log(history_id):
    """Download the log file"""
    # If it's a fake record ID (9999), handle differently
    if history_id == 9999:
        log_path = request.args.get('log_path')
        if log_path:
            return redirect(url_for('download_log_file', log_path=log_path))
        flash("Invalid log file.", "warning")
        return redirect(url_for('reports'))
    
    record = BackupHistory.query.get_or_404(history_id)
    
    if not record.log_path:
        flash("No log file available for download.", "warning")
        return redirect(url_for('reports'))
    
    log_path = Path(record.log_path).resolve()
    logs_root = (Path(app.root_path) / 'backup_logs').resolve()
    
    try:
        log_path.relative_to(logs_root)
    except ValueError:
        abort(404)
    
    if not log_path.exists() or not log_path.is_file():
        flash("The log file could not be found.", "warning")
        return redirect(url_for('reports'))
    
    return send_file(log_path, as_attachment=True, download_name=log_path.name)

@app.route('/logs/download-file')
def download_log_file():
    """Download a log file directly by path"""
    log_path = request.args.get('log_path')
    if not log_path:
        flash("No log file specified.", "warning")
        return redirect(url_for('reports'))
    
    log_path = Path(log_path).resolve()
    logs_root = (Path(app.root_path) / 'backup_logs').resolve()
    
    try:
        log_path.relative_to(logs_root)
    except ValueError:
        abort(404)
    
    if not log_path.exists() or not log_path.is_file():
        flash("The log file could not be found.", "warning")
        return redirect(url_for('reports'))
    
    return send_file(log_path, as_attachment=True, download_name=log_path.name)
# ============================================
# SCHEDULER MANAGEMENT ROUTES
# ============================================

@app.route('/scheduler/status')
def scheduler_status():
    from scheduler import get_scheduler_status
    status = get_scheduler_status()
    return jsonify(status)

@app.route('/scheduler/reschedule')
def reschedule_jobs():
    from scheduler import reschedule_all_jobs
    reschedule_all_jobs()
    return jsonify({'success': True, 'message': 'All jobs rescheduled'})

@app.route('/scheduler/pause')
def pause_scheduler():
    from scheduler import pause_scheduler as pause_sched
    pause_sched()
    return jsonify({'success': True, 'message': 'Scheduler paused'})

@app.route('/scheduler/resume')
def resume_scheduler():
    from scheduler import resume_scheduler as resume_sched
    resume_sched()
    return jsonify({'success': True, 'message': 'Scheduler resumed'})

# ============================================
# DEBUG ROUTES
# ============================================

@app.route('/debug/scheduler')
def debug_scheduler():
    from scheduler import get_scheduler_status, get_scheduled_jobs
    status = get_scheduler_status()
    jobs = get_scheduled_jobs()
    
    # Get all backup jobs from database
    db_jobs = BackupJob.query.filter_by(is_active=True).all()
    
    debug_info = {
        'scheduler_status': status,
        'scheduled_jobs': jobs,
        'database_jobs': [
            {
                'id': job.id,
                'name': job.name,
                'schedule_entries': get_schedule_entries(
                    normalize_schedule_config(
                        job.schedule_config,
                        job.schedule_type,
                        job.cron_expression,
                        fallback_retention=job.retention_policy,
                    )
                ),
                'is_active': job.is_active,
                'databases': json.loads(job.databases) if job.databases else []
            }
            for job in db_jobs
        ]
    }
    
    return jsonify(debug_info)

@app.route('/test/scheduler/<int:job_id>')
def test_scheduler(job_id):
    """Test if scheduler can trigger a specific job"""
    try:
        print(f"🧪 Testing scheduler for job ID: {job_id}")
        queued = enqueue_immediate_backup_job(scheduler, job_id)
        return jsonify({
            'success': queued,
            'message': f'Test trigger queued for job {job_id}' if queued else 'Scheduler is not running'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        })

@app.route('/debug/cron/<int:job_id>')
def debug_cron(job_id):
    job = BackupJob.query.get_or_404(job_id)
    
    from apscheduler.triggers.cron import CronTrigger
    try:
        schedule_entries = get_schedule_entries(
            normalize_schedule_config(
                job.schedule_config,
                job.schedule_type,
                job.cron_expression,
                fallback_retention=job.retention_policy,
            )
        )
        trigger_data = []

        for entry in schedule_entries:
            parts = entry['cron_expression'].split()
            trigger = CronTrigger(
                minute=parts[0],
                hour=parts[1],
                day=parts[2],
                month=parts[3],
                day_of_week=parts[4]
            )
            next_run = trigger.get_next_fire_time(None, datetime.now())
            trigger_data.append({
                'schedule_type': entry['type'],
                'cron_expression': entry['cron_expression'],
                'summary': entry['summary'],
                'next_scheduled_run': next_run.isoformat() if next_run else None,
            })

        return jsonify({
            'success': True,
            'job_name': job.name,
            'schedules': trigger_data,
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error parsing cron: {str(e)}'
        })

@app.route('/debug/scheduler-status')
def debug_scheduler_status():
    """Debug endpoint to check scheduler status"""
    from scheduler import get_scheduler_status, get_scheduled_jobs
    
    status = get_scheduler_status()
    jobs = get_scheduled_jobs()
    
    # Get database jobs for comparison
    db_jobs = BackupJob.query.filter_by(is_active=True).all()
    
    return jsonify({
        'scheduler': status,
        'scheduled_jobs': jobs,
        'database_jobs': [
            {
                'id': job.id,
                'name': job.name,
                'schedule_entries': get_schedule_entries(
                    normalize_schedule_config(
                        job.schedule_config,
                        job.schedule_type,
                        job.cron_expression,
                        fallback_retention=job.retention_policy,
                    )
                ),
                'is_active': job.is_active,
                'databases': json.loads(job.databases) if job.databases else []
            }
            for job in db_jobs
        ],
        'lock_files': {
            'app_lock_exists': os.path.exists('/tmp/backup_app.lock'),
            'scheduler_lock_exists': os.path.exists('/tmp/backup_scheduler.lock'),
            'job_locks': len([f for f in os.listdir('/tmp') if f.startswith('backup_job_') and f.endswith('.lock')])
        }
    })

@app.route('/debug/force-scheduler-start')
def force_scheduler_start():
    """Force scheduler to start (for testing)"""
    global scheduler
    
    from scheduler import init_scheduler, get_scheduler_status
    
    print("🔄 Forcing scheduler start...")
    scheduler = init_scheduler(app)
    
    if scheduler:
        # Schedule any active jobs
        jobs = BackupJob.query.filter_by(is_active=True).all()
        for job in jobs:
            schedule_backup_job(scheduler, job)
        
        status = get_scheduler_status()
        return jsonify({
            'success': True,
            'message': 'Scheduler started successfully',
            'status': status
        })
    else:
        return jsonify({
            'success': False,
            'message': 'Failed to start scheduler'
        })

@app.route('/debug/next-runs')
def debug_next_runs():
    """Check when jobs are scheduled to run next"""
    from scheduler import get_scheduled_jobs
    from apscheduler.triggers.cron import CronTrigger
    from datetime import datetime
    
    jobs_info = []
    db_jobs = BackupJob.query.filter_by(is_active=True).all()
    
    for job in db_jobs:
        try:
            schedule_entries = get_schedule_entries(
                normalize_schedule_config(
                    job.schedule_config,
                    job.schedule_type,
                    job.cron_expression,
                    fallback_retention=job.retention_policy,
                )
            )
            next_runs = []

            for entry in schedule_entries:
                parts = entry['cron_expression'].split()
                trigger = CronTrigger(
                    minute=parts[0],
                    hour=parts[1],
                    day=parts[2],
                    month=parts[3],
                    day_of_week=parts[4]
                )
                next_run = trigger.get_next_fire_time(None, datetime.now())
                next_runs.append({
                    'schedule_type': entry['type'],
                    'cron_expression': entry['cron_expression'],
                    'summary': entry['summary'],
                    'next_run_calculated': next_run.isoformat() if next_run else None,
                    'next_run_utc': next_run.strftime('%Y-%m-%d %H:%M:%S UTC') if next_run else None,
                    'next_run_local': next_run.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z') if next_run else None,
                })

            jobs_info.append({
                'id': job.id,
                'name': job.name,
                'schedules': next_runs,
            })
        except Exception as e:
            jobs_info.append({
                'id': job.id,
                'name': job.name,
                'error': str(e)
            })
    
    return jsonify({
        'current_time_utc': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'),
        'current_time_local': datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z'),
        'jobs': jobs_info
    })

@app.route('/debug/scheduler-ping')
def scheduler_ping():
    """Simple ping to check if scheduler is alive"""
    from scheduler import get_scheduler_status
    status = get_scheduler_status()
    
    # Also try to manually check scheduler state
    global scheduler
    scheduler_info = {
        'global_scheduler_exists': scheduler is not None,
        'global_scheduler_running': scheduler.running if scheduler else False,
        'status_from_function': status
    }
    
    return jsonify(scheduler_info)

# ============================================
# ERROR HANDLERS
# ============================================

@app.errorhandler(404)
def not_found_error(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    db.session.rollback()
    return render_template('500.html'), 500

# ============================================
# CLEANUP ON SHUTDOWN
# ============================================

import atexit

@atexit.register
def shutdown_on_exit():
    """Shutdown scheduler when application exits"""
    global scheduler, instance_lock_file
    
    print("🛑 Application is shutting down...")
    
    if scheduler:
        try:
            from scheduler import shutdown_scheduler
            shutdown_scheduler()
        except Exception as e:
            print(f"⚠️ Error during scheduler shutdown: {e}")
    
    if instance_lock_file and not instance_lock_file.closed:
        try:
            fcntl.flock(instance_lock_file, fcntl.LOCK_UN)
            instance_lock_file.close()
            if os.path.exists('/tmp/backup_app.lock'):
                os.unlink('/tmp/backup_app.lock')
        except Exception as e:
            pass

if __name__ == '__main__':
    try:
        app.run(debug=True, use_reloader=False, host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        # Cleanup on exit
        if instance_lock_file and not instance_lock_file.closed:
            try:
                fcntl.flock(instance_lock_file, fcntl.LOCK_UN)
                instance_lock_file.close()
                if os.path.exists('/tmp/backup_app.lock'):
                    os.unlink('/tmp/backup_app.lock')
            except:
                pass