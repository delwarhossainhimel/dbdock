from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, session
from models import db, DatabaseServer, StorageLocation, BackupJob, BackupHistory, DatabaseType, StorageType
# In your main application
from backup_scripts import postgres_backup, mysql_backup
from werkzeug.security import check_password_hash, generate_password_hash
from dotenv import load_dotenv
from scheduler import init_scheduler, schedule_backup_job, unschedule_backup_job
import json
import os
import fcntl
import sys
from datetime import datetime
load_dotenv()
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///dbDock.db'
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

# Initialize the application
with app.app_context():
    db.create_all()
    
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
    return jsonify({
        "status": "healthy",
        "message": "Application is running fine"
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
    # your existing dashboard logic here
    servers = 5
    locations = 3
    jobs = 8
    history = []  # Example data
    servers = DatabaseServer.query.filter_by(is_active=True).count()
    locations = StorageLocation.query.filter_by(is_active=True).count()
    jobs = BackupJob.query.filter_by(is_active=True).count()
    recent_history = BackupHistory.query.order_by(BackupHistory.start_time.desc()).limit(10).all()
    
    return render_template('dashboard.html', 
                          servers=servers, 
                          locations=locations, 
                          jobs=jobs,
                          history=recent_history)

# Database Servers Routes
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

# Storage Locations Routes
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
# Edit storage location - show form
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

# Edit storage location - process form
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

# Backup Jobs Routes
@app.route('/backup_jobs', methods=['GET', 'POST'])
def backup_jobs():
    if request.method == 'POST':
        name = request.form.get('name')
        description = request.form.get('description')
        database_server_id = request.form.get('database_server')
        databases = request.form.getlist('databases')
        storage_location_id = request.form.get('storage_location')
        folder_path = request.form.get('folder_path')
        schedule_type = request.form.get('schedule_type')
        retention_policy = request.form.get('retention_policy')
        notification_email = request.form.get('notification_email')
        
        # Generate cron expression based on schedule type
        cron_expression = generate_cron_expression(schedule_type, request.form)
        
        job = BackupJob(
            name=name,
            description=description,
            database_server_id=database_server_id,
            databases=json.dumps(databases),
            storage_location_id=storage_location_id,
            folder_path=folder_path,
            schedule_type=schedule_type,
            cron_expression=cron_expression,
            retention_policy=retention_policy,
            notification_email=notification_email
        )
        
        db.session.add(job)
        db.session.commit()
        
        # Schedule the job
        schedule_backup_job(scheduler, job)
        
        return redirect(url_for('backup_jobs'))
    
    servers = DatabaseServer.query.filter_by(is_active=True).all()
    locations = StorageLocation.query.filter_by(is_active=True).all()
    jobs = BackupJob.query.all()
    return render_template('backup_jobs.html', 
                          servers=servers, 
                          locations=locations, 
                          jobs=jobs)

@app.route('/delete_job/<int:job_id>', methods=['POST'])
def delete_job(job_id):
    job = BackupJob.query.get_or_404(job_id)
    
    # Remove the job from scheduler
    unschedule_backup_job(scheduler, job_id)
    
    db.session.delete(job)
    db.session.commit()
    
    return jsonify({'success': True, 'message': 'Backup job deleted successfully'})

@app.route('/edit_job/<int:job_id>', methods=['GET'])
def edit_job(job_id):
    job = BackupJob.query.get_or_404(job_id)
    servers = DatabaseServer.query.filter_by(is_active=True).all()
    locations = StorageLocation.query.filter_by(is_active=True).all()
    
    # Parse the databases from JSON
    databases = json.loads(job.databases) if job.databases else []
    
    return render_template('edit_job.html', 
                         job=job, 
                         servers=servers, 
                         locations=locations,
                         selected_databases=databases)

@app.route('/edit_job/<int:job_id>', methods=['POST'])
def update_job(job_id):
    job = BackupJob.query.get_or_404(job_id)
    
    job.name = request.form.get('name')
    job.description = request.form.get('description')
    job.database_server_id = request.form.get('database_server')
    job.databases = json.dumps(request.form.getlist('databases'))
    job.storage_location_id = request.form.get('storage_location')
    job.folder_path = request.form.get('folder_path')
    job.schedule_type = request.form.get('schedule_type')
    job.retention_policy = request.form.get('retention_policy')
    job.notification_email = request.form.get('notification_email')
    
    # Generate new cron expression
    job.cron_expression = generate_cron_expression(job.schedule_type, request.form)
    
    db.session.commit()
    
    # Reschedule the job
    schedule_backup_job(scheduler, job)
    
    return redirect(url_for('backup_jobs'))

@app.route('/run_job/<int:job_id>', methods=['POST'])
def run_job(job_id):
    from scheduler import run_backup_job
    
    # Run the job immediately
    run_backup_job(job_id)
    
    return jsonify({'success': True, 'message': 'Backup job started successfully'})

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

# API Routes
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
        
        return jsonify({'success': True, 'databases': databases})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# Reports Route
@app.route('/reports')
def reports():
    job_id = request.args.get('job_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    query = BackupHistory.query
    
    if job_id:
        query = query.filter_by(backup_job_id=job_id)
    
    if start_date:
        query = query.filter(BackupHistory.start_time >= datetime.strptime(start_date, '%Y-%m-%d'))
    
    if end_date:
        query = query.filter(BackupHistory.start_time <= datetime.strptime(end_date, '%Y-%m-%d'))
    
    history = query.order_by(BackupHistory.start_time.desc()).all()
    jobs = BackupJob.query.all()
    
    return render_template('reports.html', history=history, jobs=jobs)

# Scheduler Management Routes
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

# Debug Routes
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
                'schedule_type': job.schedule_type,
                'cron_expression': job.cron_expression,
                'is_active': job.is_active
            }
            for job in db_jobs
        ]
    }
    
    return jsonify(debug_info)

@app.route('/test/scheduler/<int:job_id>')
def test_scheduler(job_id):
    """Test if scheduler can trigger a specific job"""
    from scheduler import run_backup_job
    
    try:
        print(f"🧪 Testing scheduler for job ID: {job_id}")
        run_backup_job(job_id)
        return jsonify({
            'success': True, 
            'message': f'Test trigger sent for job {job_id}'
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
        # Parse the cron expression
        parts = job.cron_expression.split()
        if len(parts) == 5:
            trigger = CronTrigger(
                minute=parts[0],
                hour=parts[1],
                day=parts[2],
                month=parts[3],
                day_of_week=parts[4]
            )
            next_run = trigger.get_next_fire_time(None, datetime.now())
            return jsonify({
                'success': True,
                'job_name': job.name,
                'cron_expression': job.cron_expression,
                'next_scheduled_run': next_run.isoformat() if next_run else None,
                'schedule_type': job.schedule_type
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Invalid cron expression: {job.cron_expression}'
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
                'cron_expression': job.cron_expression,
                'is_active': job.is_active
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
            # Parse cron expression to calculate next run
            parts = job.cron_expression.split()
            if len(parts) == 5:
                trigger = CronTrigger(
                    minute=parts[0],
                    hour=parts[1],
                    day=parts[2],
                    month=parts[3],
                    day_of_week=parts[4]
                )
                next_run = trigger.get_next_fire_time(None, datetime.now())
                
                jobs_info.append({
                    'id': job.id,
                    'name': job.name,
                    'cron_expression': job.cron_expression,
                    'next_run_calculated': next_run.isoformat() if next_run else None,
                    'next_run_utc': next_run.strftime('%Y-%m-%d %H:%M:%S UTC') if next_run else None,
                    'next_run_local': next_run.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z') if next_run else None,
                    'time_until_next': str(next_run - datetime.now().astimezone()) if next_run else None
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

# Utility Functions
def generate_cron_expression(schedule_type, form_data):
    if schedule_type == 'daily':
        time_str = form_data.get('daily_time', '00:00')
        hour = time_str.split(':')[0]
        minute = time_str.split(':')[1]
        return f"{minute} {hour} * * *"
    elif schedule_type == 'weekly':
        time_str = form_data.get('weekly_time', '00:00')
        hour = time_str.split(':')[0]
        minute = time_str.split(':')[1]
        day_of_week = form_data.get('weekly_day', '0')
        return f"{minute} {hour} * * {day_of_week}"
    elif schedule_type == 'monthly':
        time_str = form_data.get('monthly_time', '00:00')
        hour = time_str.split(':')[0]
        minute = time_str.split(':')[1]
        day_of_month = form_data.get('monthly_day', '1')
        return f"{minute} {hour} {day_of_month} * *"
    return None

# Error Handlers
@app.errorhandler(404)
def not_found_error(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    db.session.rollback()
    return render_template('500.html'), 500

# Cleanup on app shutdown
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