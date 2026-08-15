from apscheduler.schedulers.background import BackgroundScheduler
from flask_apscheduler import APScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from models import db, BackupHistory
from backup_scripts.mysql_backup import mysql_backup
from backup_scripts.postgres_backup import postgres_backup
from job_schedules import (
    get_enabled_schedule_types,
    get_schedule_entries,
    matches_schedule,
    normalize_schedule_config,
    cron_dow_to_apscheduler,  # ← ADD THIS
    apscheduler_dow_to_cron,  # ← ADD THIS (optional)
    cron_dow_to_display,      # ← ADD THIS (optional)
)
import json
from datetime import datetime, timedelta, timezone
import atexit
from contextlib import redirect_stderr, redirect_stdout
import fcntl
import html
import os
import smtplib
import ssl
import sys
import time
import threading
import uuid
import signal
import psutil
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from log_rotation import rotate_log, cleanup_old_rotated_logs


# Global variable to track if scheduler is already running
scheduler = None
scheduler_lock_file = None
SCHEDULER_LOCK_PATH = '/tmp/backup_scheduler.lock'
BACKUP_LOG_DIR = 'backup_logs'
# Store running job processes for cancellation
running_jobs = {}
running_job_processes = {}
cancellation_flags = {}

# Global dictionary to track running processes
# Key: job_id_schedule_type, Value: {'process': process, 'history_id': history_id}
running_backup_processes = {}
running_backup_processes_lock = threading.Lock()

class TeeStream:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)
            stream.flush()
        return len(data)

    def flush(self):
        for stream in self.streams:
            stream.flush()
class SafeTeeStream:
    def __init__(self, *streams):
        self.streams = []
        for stream in streams:
            if stream and not getattr(stream, 'closed', True):
                self.streams.append(stream)

    def write(self, data):
        valid_streams = []
        for stream in self.streams:
            try:
                if not getattr(stream, 'closed', False):
                    stream.write(data)
                    stream.flush()
                    valid_streams.append(stream)
            except (ValueError, OSError, AttributeError):
                # Stream is closed or invalid, skip it
                pass
        self.streams = valid_streams
        return len(data)

    def flush(self):
        valid_streams = []
        for stream in self.streams:
            try:
                if not getattr(stream, 'closed', False):
                    stream.flush()
                    valid_streams.append(stream)
            except (ValueError, OSError, AttributeError):
                pass
        self.streams = valid_streams

def register_backup_process(job_id, trigger_source, process, history_id):
    """Register a running backup process for cancellation"""
    key = f"{job_id}_{trigger_source}"
    with running_backup_processes_lock:
        running_backup_processes[key] = {
            'process': process,
            'history_id': history_id,
            'started_at': datetime.now()
        }
    print(f"📝 Registered process for job {key} (PID: {process.pid})")


def unregister_backup_process(job_id, trigger_source):
    """Unregister a completed backup process"""
    key = f"{job_id}_{trigger_source}"
    with running_backup_processes_lock:
        if key in running_backup_processes:
            del running_backup_processes[key]
            print(f"🧹 Unregistered process for job {key}")

def cancel_backup_job(job_id, trigger_source=None):
    """
    Cancel a running backup job by killing the actual process
    """
    from app import app
    
    with app.app_context():
        from models import BackupHistory, db
        from datetime import datetime
        
        # Build the key
        if trigger_source:
            key = f"{job_id}_{trigger_source}"
        else:
            key = str(job_id)
        
        # Find the running history record FIRST
        query = BackupHistory.query.filter(
            BackupHistory.backup_job_id == job_id,
            BackupHistory.status == 'running'
        )
        if trigger_source:
            query = query.filter(BackupHistory.trigger_source == trigger_source)
        
        history = query.order_by(BackupHistory.start_time.desc()).first()
        
        if not history:
            print(f"❌ No running history found for job {job_id}")
            return False
        
        # Check if there's a running process
        process_info = None
        with running_backup_processes_lock:
            # Try exact match first
            if key in running_backup_processes:
                process_info = running_backup_processes[key]
            else:
                # Try to find by job_id only (any schedule type)
                for k, v in running_backup_processes.items():
                    if k.startswith(f"{job_id}_"):
                        process_info = v
                        key = k
                        break
        
        # Kill the process if found
        if process_info and process_info.get('process'):
            try:
                process = process_info['process']
                pid = process.pid
                
                print(f"🛑 Attempting to kill process PID: {pid} for job {key}")
                
                # Kill the entire process tree
                try:
                    import psutil
                    parent = psutil.Process(pid)
                    children = parent.children(recursive=True)
                    
                    # Kill children first (mysqldump/pg_dump processes)
                    for child in children:
                        try:
                            print(f"   Killing child process PID: {child.pid}")
                            child.kill()
                        except Exception as e:
                            print(f"   ⚠️ Error killing child {child.pid}: {e}")
                    
                    # Kill parent process
                    parent.kill()
                    print(f"✅ Killed process PID: {pid}")
                    
                except ImportError:
                    # Fallback if psutil not installed
                    import signal
                    os.killpg(os.getpgid(pid), signal.SIGTERM)
                    print(f"✅ Killed process group PID: {pid}")
                    
                except psutil.NoSuchProcess:
                    print(f"⚠️ Process {pid} no longer exists")
                except Exception as e:
                    print(f"⚠️ Error killing process tree: {e}")
                
                # Update the history record to CANCELLED
                history.status = 'cancelled'
                history.is_cancelled = True
                history.cancelled_at = datetime.now()
                history.cancelled_by = 'User'
                history.message = 'Job cancelled by user (process killed)'
                db.session.commit()
                print(f"✅ Job {key} marked as CANCELLED in database")
                
                # Clean up the process info
                with running_backup_processes_lock:
                    if key in running_backup_processes:
                        del running_backup_processes[key]
                
                return True
                
            except Exception as e:
                print(f"❌ Error killing process: {e}")
                # Still mark as cancelled even if process kill fails
                history.status = 'cancelled'
                history.is_cancelled = True
                history.cancelled_at = datetime.now()
                history.cancelled_by = 'User'
                history.message = f'Job cancelled by user (process kill failed: {str(e)})'
                db.session.commit()
                return True
        
        # If no process found, still mark as cancelled
        history.status = 'cancelled'
        history.is_cancelled = True
        history.cancelled_at = datetime.now()
        history.cancelled_by = 'User'
        history.message = 'Job cancelled by user (no process found)'
        db.session.commit()
        print(f"✅ Job {key} marked as CANCELLED in database (no process found)")
        
        # Clean up process info if exists
        with running_backup_processes_lock:
            if key in running_backup_processes:
                del running_backup_processes[key]
        
        return True
    
def cleanup_stuck_jobs(max_hours=48):
    """
    Clean up jobs that have been running for too long.
    Default: 48 hours (configurable)
    """
    from app import app
    from datetime import datetime, timedelta
    
    with app.app_context():
        from models import BackupHistory, db
        
        # Use environment variable or default to 48 hours
        timeout_hours = int(os.getenv('JOB_TIMEOUT_HOURS', max_hours))
        timeout = datetime.now() - timedelta(hours=timeout_hours)
        
        print(f"🧹 Running cleanup for jobs older than {timeout_hours} hours")
        
        stuck_jobs = BackupHistory.query.filter(
            BackupHistory.status == 'running',
            BackupHistory.start_time < timeout
        ).all()
        
        if not stuck_jobs:
            print("   No stuck jobs found")
            return
        
        for job in stuck_jobs:
            duration = (datetime.now() - job.start_time).total_seconds() / 3600
            print(f"🔄 Cleaning up stuck job: {job.id} (running for {duration:.1f} hours, started at {job.start_time})")
            
            # Kill the process if still running
            key = f"{job.backup_job_id}_{job.trigger_source}"
            with running_backup_processes_lock:
                if key in running_backup_processes:
                    try:
                        process = running_backup_processes[key]['process']
                        process.kill()
                        print(f"   ✅ Killed stuck process PID: {process.pid}")
                    except Exception as e:
                        print(f"   ⚠️ Error killing process: {e}")
                    # Always remove from tracking
                    del running_backup_processes[key]
                else:
                    print(f"   ⚠️ No process found for job {key}")
            
            job.status = 'failed'
            job.end_time = datetime.now()
            job.message = f"Job timed out after {timeout_hours} hours. Started at {job.start_time}"
            db.session.commit()
            print(f"   ✅ Updated job {job.id} status to 'failed'")

def is_job_cancelled(job_id, trigger_source):
    """
    Check if a job has been cancelled
    """
    key = f"{job_id}_{trigger_source}"
    return cancellation_flags.get(key, False)


def clear_cancellation_flag(job_id, trigger_source):
    """
    Clear the cancellation flag after a job finishes
    """
    key = f"{job_id}_{trigger_source}"
    if key in cancellation_flags:
        del cancellation_flags[key]
        print(f"🧹 Cleared cancellation flag for {key}")

def init_scheduler(app):
    global scheduler, scheduler_lock_file
    timezone = app.config.get('APP_TIMEZONE', 'Asia/Dhaka')

    # Prevent multiple scheduler instances
    if scheduler and scheduler.running:
        print("✅ Scheduler is already running")
        return scheduler
    
    # Create a lock file to prevent multiple instances
    try:
        scheduler_lock_file = open(SCHEDULER_LOCK_PATH, 'w')
        fcntl.flock(scheduler_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print("🔒 Scheduler lock acquired")
    except IOError:
        print("❌ Another scheduler instance is already running")
        return scheduler
    except Exception as e:
        print(f"❌ Error creating scheduler lock: {e}")
        return None
    
    try:
        # Configure job stores and executors
        jobstores = {
            'default': MemoryJobStore()
        }
        executors = {
            'default': ThreadPoolExecutor(5)
        }
        job_defaults = {
            'coalesce': True,  # Combine multiple pending executions
            'max_instances': 1,  # Only one instance of a job can run at a time
            'misfire_grace_time': 300  # 5 minutes grace period
        }
        
        # Initialize scheduler
        scheduler = APScheduler(BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='Asia/Dhaka'
        ))
        
        scheduler.init_app(app)
        
        # Register shutdown handler
        atexit.register(shutdown_scheduler)
        
        # Start scheduler
        if not scheduler.running:
            scheduler.start()
            print("✅ Scheduler started successfully")
        
        return scheduler
        
    except Exception as e:
        print(f"❌ Error initializing scheduler: {e}")
        import traceback
        traceback.print_exc()
        shutdown_scheduler()
        return None

def shutdown_scheduler():
    global scheduler, scheduler_lock_file
    if scheduler:
        try:
            if scheduler.running:
                scheduler.shutdown()
                print("🛑 Scheduler shut down")
            else:
                print("ℹ️  Scheduler was not running, no need to shut down")
        except Exception as e:
            print(f"⚠️ Error shutting down scheduler: {e}")
    
    if scheduler_lock_file:
        try:
            # Only try to unlock if the file is still open
            if not scheduler_lock_file.closed:
                fcntl.flock(scheduler_lock_file, fcntl.LOCK_UN)
                scheduler_lock_file.close()
            # Clean up the lock file
            if os.path.exists(SCHEDULER_LOCK_PATH):
                os.unlink(SCHEDULER_LOCK_PATH)
        except Exception as e:
            # It's okay if the file is already closed or doesn't exist
            pass

def schedule_backup_job(scheduler_obj, job):
    """Schedule a backup job with proper configuration"""
    if not scheduler_obj:
        print("❌ Scheduler not available for scheduling job")
        return
    
    # Remove existing job if it exists
    unschedule_backup_job(scheduler_obj, job.id)
    
    schedule_config = normalize_schedule_config(
        job.schedule_config,
        job.schedule_type,
        job.cron_expression,
        fallback_retention=job.retention_policy,
    )
    schedule_entries = get_schedule_entries(schedule_config)

    if not schedule_entries:
        print(f"❌ No enabled schedules found for job: {job.name}")
        return
    
    try:
        for entry in schedule_entries:
            cron_parts = entry["cron_expression"].split()
            minute, hour, day, month, day_of_week = cron_parts
            # Convert day_of_week from cron format to APScheduler format
            # Cron: 0=Sunday, 6=Saturday
            # APScheduler: 0=Monday, 6=Sunday
            if day_of_week != '*':
                cron_dow = int(day_of_week)
                aps_dow = cron_dow_to_apscheduler(cron_dow)
                print(f"🔄 Converting day_of_week: {cron_dow} (cron) -> {aps_dow} (APScheduler)")
                day_of_week = str(aps_dow)
            print(f"🔍 DEBUG Cron parts for {entry['type']}:")
            print(f"   minute: {minute}")
            print(f"   hour: {hour}")
            print(f"   day: {day}")
            print(f"   month: {month}")
            print(f"   day_of_week: {day_of_week}")
            scheduler_obj.add_job(
                id=get_schedule_job_id(job.id, entry["type"]),
                func=run_backup_job,
                args=[job.id, entry["type"]],
                trigger='cron',
                minute=minute,
                hour=hour,
                day=day if day != '*' else None,
                month=month if month != '*' else None,
                day_of_week=day_of_week if day_of_week != '*' else None,
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                misfire_grace_time=300
            )

        print(f"✅ Successfully scheduled job: {job.name} (ID: {job.id})")
        print(f"   📅 Schedules: {[entry['summary'] for entry in schedule_entries]}")
        print(f"   🗃️  Databases: {json.loads(job.databases)}")
        print(f"   ⏰ Next runs: {get_next_run_time(scheduler_obj, job.id)}")
        
    except Exception as e:
        print(f"❌ Error scheduling job {job.name}: {e}")
        import traceback
        traceback.print_exc()

def get_next_run_time(scheduler_obj, job_id):
    """Get the next run time for a job"""
    try:
        next_runs = []
        for scheduled_job in scheduler_obj.get_jobs():
            if scheduled_job.id.startswith(f'backup_job_{job_id}_'):
                if hasattr(scheduled_job, 'next_run_time') and scheduled_job.next_run_time:
                    schedule_type = scheduled_job.id.replace(f'backup_job_{job_id}_', '')
                    next_time = scheduled_job.next_run_time
                    
                    # Convert to local timezone if needed
                    if next_time.tzinfo is None:
                        import pytz
                        local_tz = pytz.timezone('Asia/Dhaka')
                        next_time = local_tz.localize(next_time)
                    
                    next_runs.append(
                        f"{schedule_type.capitalize()}: "
                        f"{next_time.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    )

        return next_runs or ["Calculating..."]
    except Exception as e:
        return f"Error: {str(e)}"

def unschedule_backup_job(scheduler_obj, job_id):
    """Remove a backup job from the scheduler"""
    if not scheduler_obj:
        return
        
    try:
        for scheduled_job in scheduler_obj.get_jobs():
            if scheduled_job.id.startswith(f'backup_job_{job_id}_'):
                scheduler_obj.remove_job(scheduled_job.id)
                print(f"✅ Unscheduled job: {scheduled_job.id}")
    except Exception as e:
        # Job might not exist, which is fine
        pass

def get_schedule_job_id(job_id, schedule_type):
    return f'backup_job_{job_id}_{schedule_type}'

def enqueue_immediate_backup_job(scheduler_obj, job_id):
    if scheduler_obj and scheduler_obj.running:
        scheduler_obj.add_job(
            id=f'immediate_backup_job_{job_id}_{uuid.uuid4().hex}',
            func=run_backup_job,
            args=[job_id, 'manual'],
            trigger='date',
            run_date=datetime.now(timezone.utc),
            replace_existing=False
        )
        return True

    threading.Thread(target=run_backup_job, args=(job_id, 'manual'), daemon=True).start()
    return True

def determine_run_targets(job, trigger_source):
    schedule_config = normalize_schedule_config(
        job.schedule_config,
        job.schedule_type,
        job.cron_expression,
        fallback_retention=job.retention_policy,
    )
    enabled_schedule_types = get_enabled_schedule_types(schedule_config)

    if trigger_source == 'manual':
        return enabled_schedule_types or ['manual']

    if trigger_source in enabled_schedule_types:
        # Use local time from the scheduler
        from datetime import datetime
        import pytz
        local_tz = pytz.timezone('Asia/Dhaka')
        current_time = datetime.now(local_tz)
        
        matched_types = [
            schedule_type
            for schedule_type in enabled_schedule_types
            if matches_schedule(schedule_type, schedule_config[schedule_type], current_time)
        ]
        if trigger_source not in matched_types:
            matched_types.insert(0, trigger_source)
        return matched_types

    return enabled_schedule_types or ['manual']

def run_backup_job(job_id, trigger_source='manual'):
    """
    Run backup job with cancellation support and timeout checking
    """
    key = f"{job_id}_{trigger_source}"
    
    print(f"🔹 Starting backup job ID: {job_id} (trigger: {trigger_source})")
    
    # Clear any previous cancellation flag
    if key in cancellation_flags:
        del cancellation_flags[key]
    
    # Create a lock file for this specific job
    lock_file = f"/tmp/backup_job_{job_id}.lock"
    lock_fd = None
    
    try:
        # Try to acquire lock
        lock_fd = os.open(lock_file, os.O_CREAT | os.O_WRONLY)
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        
    except IOError:
        print(f"⏸️ Job {job_id} is already running, skipping...")
        return
    except Exception as e:
        print(f"❌ Error acquiring lock for job {job_id}: {e}")
        return
    
    try:
        from app import app
        
        with app.app_context():
            from models import BackupJob, DatabaseServer, StorageLocation, BackupHistory, db
            
            # Check if cancelled before starting
            if is_job_cancelled(job_id, trigger_source):
                print(f"🛑 Job {job_id} ({trigger_source}) was cancelled before starting")
                # Update history to cancelled
                history = BackupHistory.query.filter(
                    BackupHistory.backup_job_id == job_id,
                    BackupHistory.status == 'running'
                ).order_by(BackupHistory.start_time.desc()).first()
                
                if history:
                    history.status = 'cancelled'
                    history.is_cancelled = True
                    history.cancelled_at = datetime.now()
                    history.message = 'Job cancelled by user before execution'
                    db.session.commit()
                
                clear_cancellation_flag(job_id, trigger_source)
                return
            
            job = BackupJob.query.get(job_id)
            if not job or not job.is_active:
                print(f"❌ Job {job_id} not found or inactive")
                return
            
            # Check if there's already a running instance of this job in the database
            running_job = BackupHistory.query.filter(
                BackupHistory.backup_job_id == job.id,
                BackupHistory.status == 'running',
                BackupHistory.start_time > datetime.now() - timedelta(hours=1)
            ).first()
            
            if running_job:
                print(f"⏸️ Job {job.name} is already running (started at {running_job.start_time}), skipping...")
                return

            schedule_targets = determine_run_targets(job, trigger_source)

            print(f"🚀 Starting backup: {job.name}")
            print(f"   - Trigger source: {trigger_source}")
            print(f"   - Target schedules: {schedule_targets}")
            
            # Create backup history record
            history = BackupHistory(
                backup_job_id=job.id,
                start_time=datetime.now(),
                status='running',
                trigger_source=trigger_source,
                is_cancelled=False
            )
            db.session.add(history)
            db.session.commit()

            success = False
            message = "Backup did not start."
            file_path = None
            file_size = 0
            database_results = []
            
            # Setup log directory and file with schedule type
            logs_dir = os.path.join(app.root_path, BACKUP_LOG_DIR)
            os.makedirs(logs_dir, exist_ok=True)
            
            # Use schedule-specific log file
            log_filename = f"backup_job_{job.id}_{trigger_source}.log"
            log_path = os.path.join(logs_dir, log_filename)
            history.log_path = log_path
            db.session.commit()
            
            # Rotate log if needed
            rotate_log(log_path)
            cleanup_old_rotated_logs(logs_dir)
            
            run_id = f"{history.id}_{int(datetime.now().timestamp())}"
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            try:
                server = job.database_server
                location = job.storage_location
                databases = json.loads(job.databases)
                
                # Check if cancelled
                if is_job_cancelled(job_id, trigger_source):
                    print(f"🛑 Job {job_id} ({trigger_source}) cancelled during initialization")
                    history.status = 'cancelled'
                    history.is_cancelled = True
                    history.cancelled_at = datetime.now()
                    history.message = 'Job cancelled by user'
                    db.session.commit()
                    return
                
                log_file = None
                try:
                    log_file = open(log_path, 'a', encoding='utf-8')
                except Exception as e:
                    print(f"⚠️ Error opening log file: {e}")
                
                if log_file:
                    tee_stdout = SafeTeeStream(sys.stdout, log_file)
                    tee_stderr = SafeTeeStream(sys.stderr, log_file)
                else:
                    tee_stdout = SafeTeeStream(sys.stdout)
                    tee_stderr = SafeTeeStream(sys.stderr)
                
                with redirect_stdout(tee_stdout), redirect_stderr(tee_stderr):
                    print(f"\n{'='*80}")
                    print(f"🔹 BACKUP RUN STARTED: {timestamp}")
                    print(f"   Run ID: {run_id}")
                    print(f"   Job: {job.name} (ID: {job.id})")
                    print(f"   Schedule Type: {trigger_source}")
                    print(f"   Log file: {log_path}")
                    print(f"   Timeout: {os.getenv('JOB_TIMEOUT_HOURS', 48)} hours")
                    print(f"{'='*80}\n")
                    
                    print(f"📊 Backup details:")
                    print(f"   - Server: {server.name} ({server.type.value})")
                    print(f"   - Storage: {location.name} ({location.type.value})")
                    print(f"   - Databases: {databases}")
                    print(f"   - Schedule config: {schedule_targets}")
                    print(f"   - Folder: {job.folder_path}")
                    
                    # Check if cancelled before backup
                    if is_job_cancelled(job_id, trigger_source):
                        print(f"🛑 Job {job_id} ({trigger_source}) cancelled before backup execution")
                        success = False
                        message = "Job cancelled by user"
                    else:
                        # Run backup based on database type
                        if server.type.value == 'mysql':
                            success, message, file_path, file_size, database_results = mysql_backup(
                                server, databases, location, job.folder_path, schedule_targets, job.id, trigger_source
                            )
                        elif server.type.value == 'postgres':
                            success, message, file_path, file_size, database_results = postgres_backup(
                                server, databases, location, job.folder_path, schedule_targets, job.id, trigger_source
                            )
                        else:
                            success, message, file_path, file_size, database_results = False, "Unsupported database type", None, 0, []
                    
                    print(f"\n{'='*80}")
                    print(f"🏁 BACKUP RUN COMPLETED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"   Status: {'SUCCESS' if success else 'FAILED' if not is_job_cancelled(job_id, trigger_source) else 'CANCELLED'}")
                    print(f"   Message: {message}")
                    print(f"   Duration: {(datetime.now() - history.start_time).total_seconds():.2f} seconds")
                    print(f"{'='*80}\n")
                
                if log_file and not log_file.closed:
                    log_file.close()
                
                # Update history record
                history.end_time = datetime.now()
                
                # Check if cancelled during execution
                if is_job_cancelled(job_id, trigger_source):
                    history.status = 'cancelled'
                    history.is_cancelled = True
                    history.cancelled_at = datetime.now()
                    history.message = 'Job cancelled by user'
                    success = False
                else:
                    history.status = 'success' if success else 'failed'
                    history.message = message
                
                history.file_path = file_path
                history.file_size = file_size
                history.trigger_source = trigger_source
                
                duration = (history.end_time - history.start_time).total_seconds()
                
                if history.status == 'cancelled':
                    print(f"🛑 Backup cancelled: {job.name} ({trigger_source})")
                elif success:
                    print(f"✅ Backup completed successfully: {job.name} ({trigger_source})")
                    print(f"   - Duration: {duration:.2f} seconds")
                    print(f"   - File size: {file_size} bytes")
                else:
                    print(f"❌ Backup failed: {job.name} ({trigger_source})")
                    print(f"   - Error: {message}")
                    print(f"   - Duration: {duration:.2f} seconds")
                    
            except Exception as e:
                history.end_time = datetime.now()
                history.status = 'failed'
                message = f"Unexpected error: {str(e)}"
                history.message = message
                history.trigger_source = trigger_source
                
                # Check if it was cancelled
                if is_job_cancelled(job_id, trigger_source):
                    history.status = 'cancelled'
                    history.is_cancelled = True
                    history.cancelled_at = datetime.now()
                    history.message = 'Job cancelled by user'
                    print(f"🛑 Backup cancelled: {job.name} ({trigger_source})")
                else:
                    history.status = 'failed'
                    message = f"Unexpected error: {str(e)}"
                    history.message = message
                    print(f"❌ Backup failed: {job.name} ({trigger_source})")

                
                try:
                    with open(log_path, 'a', encoding='utf-8') as log_file:
                        print(f"💥 Unexpected error in backup job {job.name}: {e}", file=log_file)
                        import traceback
                        traceback.print_exc(file=log_file)
                except:
                    pass
                
                print(f"💥 Unexpected error in backup job {job.name} ({trigger_source}): {e}")
                import traceback
                traceback.print_exc()
            
            db.session.commit()

            # Clear cancellation flag
            clear_cancellation_flag(job_id, trigger_source)

            # Send notification only if not cancelled
            if history.status != 'cancelled':
                if job.notification_email or os.getenv('SMTP_RECEIVER_EMAIL'):
                    send_notification_email(job, history, schedule_targets, success, message, database_results)
            
    except Exception as e:
        print(f"💥 Critical error in run_backup_job for job {job_id} ({trigger_source}): {e}")
        import traceback
        traceback.print_exc()
        # Clear cancellation flag on error too
        clear_cancellation_flag(job_id, trigger_source)
        
    finally:
        # Release lock
        try:
            if lock_fd:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
            try:
                if os.path.exists(lock_file):
                    os.unlink(lock_file)
            except:
                pass
        except Exception as e:
            print(f"⚠️ Error releasing lock for job {job_id} ({trigger_source}): {e}")

def build_history_message(message, database_results):
    if not database_results:
        return message

    summary = summarize_database_results(database_results)
    return f"{message}\n{summary['success_count']} succeeded, {summary['failed_count']} failed out of {summary['total_count']} databases."

def summarize_database_results(database_results):
    success_count = len([item for item in database_results if item.get('status') == 'success'])
    total_count = len(database_results)
    failed_count = total_count - success_count
    return {
        'success_count': success_count,
        'failed_count': failed_count,
        'total_count': total_count,
    }

def send_notification_email(job, history, schedule_targets, success, message, database_results):
    """Send an HTML backup report email via SMTP using environment variables."""
    try:
        smtp_config = get_smtp_config(job)
        if not smtp_config['host'] or not smtp_config['receiver_email'] or not smtp_config['from_email']:
            print("⚠️ SMTP configuration is incomplete. Skipping notification email.")
            return

        schedule_label = get_email_schedule_label(schedule_targets)
        subject = f"{schedule_label} backup report - {job.name} - {'Success' if success else 'Failed'}"
        previous_backup = get_previous_successful_backup(job.id, history.id)
        html_body = build_backup_report_html(job, history, schedule_label, database_results, previous_backup)
        text_body = build_backup_report_text(job, history, schedule_label, database_results, previous_backup, message)

        email_message = MIMEMultipart('alternative')
        email_message['Subject'] = subject
        email_message['From'] = formataddr((smtp_config['from_name'], smtp_config['from_email']))
        email_message['To'] = smtp_config['receiver_email']

        email_message.attach(MIMEText(text_body, 'plain'))
        email_message.attach(MIMEText(html_body, 'html'))

        if smtp_config['use_ssl']:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'], context=context) as server:
                login_and_send(server, smtp_config, email_message)
        else:
            with smtplib.SMTP(smtp_config['host'], smtp_config['port']) as server:
                server.ehlo()
                if smtp_config['use_tls']:
                    context = ssl.create_default_context()
                    server.starttls(context=context)
                    server.ehlo()
                login_and_send(server, smtp_config, email_message)

        print(f"📧 Notification email sent to: {smtp_config['receiver_email']}")
    except Exception as e:
        print(f"⚠️ Error sending notification email: {e}")

def send_test_email(recipient_email):
    """Send a simple SMTP test email to verify configuration."""
    test_job = type('EmailTestJob', (), {'notification_email': recipient_email})()
    smtp_config = get_smtp_config(test_job)

    if not smtp_config['host'] or not smtp_config['receiver_email'] or not smtp_config['from_email']:
        raise ValueError("SMTP configuration is incomplete. Check SMTP_HOST, SMTP_FROM_EMAIL, and recipient email.")

    sent_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    subject = "SMTP Test Email - dbDock"
    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; background:#ffffff; color:#222; padding:20px;">
        <div style="max-width:720px; margin:0 auto;">
          <h2 style="text-align:center; margin-bottom:24px;">SMTP Test Email</h2>
          <table style="width:100%; border-collapse:collapse;">
            <tr>
              <td style="padding:10px; border:1px solid #d9d9d9;"><strong>Status</strong></td>
              <td style="padding:10px; border:1px solid #d9d9d9; color:#0a8f08; font-weight:700;">Success</td>
            </tr>
            <tr>
              <td style="padding:10px; border:1px solid #d9d9d9;"><strong>Sent At</strong></td>
              <td style="padding:10px; border:1px solid #d9d9d9;">{html.escape(sent_at)}</td>
            </tr>
            <tr>
              <td style="padding:10px; border:1px solid #d9d9d9;"><strong>Recipient</strong></td>
              <td style="padding:10px; border:1px solid #d9d9d9;">{html.escape(recipient_email)}</td>
            </tr>
          </table>
          <p style="text-align:center; margin-top:20px;">If you received this email, your SMTP setup is working.</p>
        </div>
      </body>
    </html>
    """
    text_body = (
        "SMTP Test Email\n\n"
        "Status: Success\n"
        f"Sent At: {sent_at}\n"
        f"Recipient: {recipient_email}\n\n"
        "If you received this email, your SMTP setup is working."
    )

    email_message = MIMEMultipart('alternative')
    email_message['Subject'] = subject
    email_message['From'] = formataddr((smtp_config['from_name'], smtp_config['from_email']))
    email_message['To'] = smtp_config['receiver_email']
    email_message.attach(MIMEText(text_body, 'plain'))
    email_message.attach(MIMEText(html_body, 'html'))

    if smtp_config['use_ssl']:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'], context=context) as server:
            login_and_send(server, smtp_config, email_message)
    else:
        with smtplib.SMTP(smtp_config['host'], smtp_config['port']) as server:
            server.ehlo()
            if smtp_config['use_tls']:
                context = ssl.create_default_context()
                server.starttls(context=context)
                server.ehlo()
            login_and_send(server, smtp_config, email_message)

def login_and_send(server, smtp_config, email_message):
    if smtp_config['username']:
        server.login(smtp_config['username'], smtp_config['password'])
    server.sendmail(
        smtp_config['from_email'],
        [smtp_config['receiver_email']],
        email_message.as_string(),
    )

def get_smtp_config(job):
    return {
        'host': os.getenv('SMTP_HOST', '').strip(),
        'port': int(os.getenv('SMTP_PORT', '587')),
        'username': os.getenv('SMTP_USER', '').strip(),
        'password': os.getenv('SMTP_PASS', ''),
        'from_name': os.getenv('SMTP_FROM_NAME', 'Infra Notification').strip(),
        'from_email': os.getenv('SMTP_FROM_EMAIL', os.getenv('SMTP_USER', '')).strip(),
        'receiver_email': (job.notification_email or os.getenv('SMTP_RECEIVER_EMAIL', '')).strip(),
        'use_tls': os.getenv('SMTP_USE_TLS', 'true').lower() == 'true',
        'use_ssl': os.getenv('SMTP_USE_SSL', 'false').lower() == 'true',
    }

def get_email_schedule_label(schedule_targets):
    if not schedule_targets:
        return "Backup"
    if len(schedule_targets) == 1:
        return schedule_targets[0].capitalize()
    return "Combined"

def get_previous_successful_backup(job_id, current_history_id):
    return (
        BackupHistory.query.filter(
            BackupHistory.backup_job_id == job_id,
            BackupHistory.status == 'success',
            BackupHistory.id != current_history_id,
        )
        .order_by(BackupHistory.start_time.desc())
        .first()
    )

def build_backup_report_html(job, history, schedule_label, database_results, previous_backup):
    summary = summarize_database_results(database_results)
    status_rows = ''.join(
        [
            (
                "<tr>"
                f"<td style=\"padding:10px;border:1px solid #d9d9d9;\">{html.escape(item.get('database', 'Unknown'))}</td>"
                f"<td style=\"padding:10px;border:1px solid #d9d9d9;color:{'#0a8f08' if item.get('status') == 'success' else '#c62828'};font-weight:700;\">"
                f"{html.escape(item.get('status', 'unknown').capitalize())}</td>"
                "</tr>"
            )
            for item in database_results
        ]
    )

    if not status_rows:
        status_rows = (
            "<tr><td style=\"padding:10px;border:1px solid #d9d9d9;\">No database rows available</td>"
            "<td style=\"padding:10px;border:1px solid #d9d9d9;\">N/A</td></tr>"
        )

    current_size = format_bytes(history.file_size or 0)
    previous_size = format_bytes(previous_backup.file_size) if previous_backup and previous_backup.file_size else "N/A"
    previous_date = previous_backup.start_time.strftime('%Y-%m-%d') if previous_backup and previous_backup.start_time else "N/A"
    detailed_path = html.escape(history.log_path or "No log file available")

    return f"""
    <html>
      <body style="font-family: Arial, sans-serif; background:#ffffff; color:#222; padding:20px;">
        <div style="max-width:760px; margin:0 auto;">
          <h2 style="text-align:center; margin-bottom:24px;">{html.escape(schedule_label)} Backup Report</h2>

          <table style="width:100%; border-collapse:collapse; margin-bottom:24px;">
            <thead>
              <tr style="background:#f2f2f2;">
                <th style="padding:10px; border:1px solid #d9d9d9; text-align:left;">Database Name</th>
                <th style="padding:10px; border:1px solid #d9d9d9; text-align:center;">Status</th>
              </tr>
            </thead>
            <tbody>
              {status_rows}
              <tr>
                <td colspan="2" style="padding:10px; border:1px solid #d9d9d9; text-align:center;">
                  <strong>Backup Summary:</strong> {summary['success_count']} succeeded, {summary['failed_count']} failed out of {summary['total_count']} databases
                </td>
              </tr>
            </tbody>
          </table>

          <h3 style="text-align:center; margin-bottom:16px;">Backup Size</h3>
          <table style="width:100%; border-collapse:collapse; margin-bottom:16px;">
            <thead>
              <tr style="background:#f2f2f2;">
                <th style="padding:10px; border:1px solid #d9d9d9;">Backup Type</th>
                <th style="padding:10px; border:1px solid #d9d9d9;">Size</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td style="padding:10px; border:1px solid #d9d9d9;"><strong>Database Backup Size of Date: {history.start_time.strftime('%Y-%m-%d')}</strong></td>
                <td style="padding:10px; border:1px solid #d9d9d9; color:#0a8f08; font-weight:700; text-align:right;">{current_size}</td>
              </tr>
              <tr>
                <td style="padding:10px; border:1px solid #d9d9d9;"><strong>Database Backup Size of Date: {previous_date}</strong></td>
                <td style="padding:10px; border:1px solid #d9d9d9; color:#0a8f08; font-weight:700; text-align:right;">{previous_size}</td>
              </tr>
            </tbody>
          </table>

          <p style="text-align:center; margin-top:20px;">Detailed log available at: {detailed_path}</p>
        </div>
      </body>
    </html>
    """

def build_backup_report_text(job, history, schedule_label, database_results, previous_backup, message):
    summary = summarize_database_results(database_results)
    lines = [
        f"{schedule_label} backup report",
        "",
        f"Job: {job.name}",
        f"Status: {'Success' if history.status == 'success' else 'Failed'}",
        f"Summary: {summary['success_count']} succeeded, {summary['failed_count']} failed out of {summary['total_count']} databases",
        "",
        "Database Status:",
    ]

    for item in database_results:
        lines.append(f"- {item.get('database', 'Unknown')}: {item.get('status', 'unknown').capitalize()}")

    lines.extend(
        [
            "",
            f"Current backup size ({history.start_time.strftime('%Y-%m-%d')}): {format_bytes(history.file_size or 0)}",
            (
                f"Previous backup size ({previous_backup.start_time.strftime('%Y-%m-%d')}): {format_bytes(previous_backup.file_size or 0)}"
                if previous_backup
                else "Previous backup size: N/A"
            ),
            f"Message: {message}",
            f"Detailed log available at: {history.log_path or 'No log file available'}",
        ]
    )

    return "\n".join(lines)

def format_bytes(size):
    if size is None:
        return "N/A"

    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)}{unit}"
            return f"{value:.1f}{unit}"
        value /= 1024

def get_scheduled_jobs():
    """
    Get list of all scheduled jobs
    """
    global scheduler
    if not scheduler:
        return []
    
    try:
        jobs = scheduler.get_jobs()
        job_list = []
        for job in jobs:
            next_run = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S UTC') if job.next_run_time else "Not scheduled"
            job_list.append({
                'id': job.id,
                'name': job.id.replace('backup_job_', ''),
                'next_run': next_run,
                'trigger': str(job.trigger)
            })
        return job_list
    except Exception as e:
        print(f"❌ Error getting scheduled jobs: {e}")
        return []

def reschedule_all_jobs():
    """
    Reschedule all active backup jobs
    Useful when restarting the application
    """
    global scheduler
    if not scheduler:
        print("❌ Scheduler not available")
        return
    
    from app import app
    from models import BackupJob
    
    with app.app_context():
        # Remove all existing jobs
        try:
            scheduler.remove_all_jobs()
            print("✅ Removed all existing jobs")
        except Exception as e:
            print(f"⚠️ Error removing existing jobs: {e}")
        
        # Schedule all active jobs
        jobs = BackupJob.query.filter_by(is_active=True).all()
        print(f"📋 Rescheduling {len(jobs)} active jobs...")
        
        scheduled_count = 0
        for job in jobs:
            try:
                schedule_backup_job(scheduler, job)
                scheduled_count += 1
            except Exception as e:
                print(f"❌ Failed to schedule job {job.name}: {e}")
        
        print(f"✅ Successfully rescheduled {scheduled_count}/{len(jobs)} jobs")

def pause_scheduler():
    """
    Pause the scheduler (stop running jobs)
    """
    global scheduler
    if scheduler and scheduler.running:
        scheduler.pause()
        print("⏸️ Scheduler paused")
    else:
        print("❌ Scheduler not running or not available")

def resume_scheduler():
    """
    Resume the scheduler
    """
    global scheduler
    if scheduler:
        scheduler.resume()
        print("▶️ Scheduler resumed")
    else:
        print("❌ Scheduler not available")

def get_scheduler_status():
    """
    Get current scheduler status
    """
    global scheduler
    if not scheduler:
        return {
            'status': 'not_initialized',
            'running': False,
            'job_count': 0,
            'error': 'Scheduler not initialized'
        }
    
    try:
        jobs = scheduler.get_jobs()
        return {
            'status': 'running' if scheduler.running else 'paused',
            'running': scheduler.running,
            'job_count': len(jobs),
            'jobs': [job.id for job in jobs],
            'next_runs': [
                {
                    'job_id': job.id,
                    'next_run': job.next_run_time.strftime('%Y-%m-%d %H:%M:%S UTC') if job.next_run_time else 'Not scheduled'
                }
                for job in jobs
            ]
        }
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'running': False,
            'job_count': 0
        }

def test_job_execution(job_id):
    """
    Test if a job can be executed (for debugging)
    """
    print(f"🧪 Testing job execution for job ID: {job_id}")
    try:
        run_backup_job(job_id)
        return True
    except Exception as e:
        print(f"❌ Job test failed: {e}")
        return False

# Clean up on module import to handle any stale lock files
def cleanup_stale_locks():
    """
    Clean up any stale lock files on startup
    """
    try:
        if os.path.exists(SCHEDULER_LOCK_PATH) and not is_lock_file_active(SCHEDULER_LOCK_PATH):
            os.unlink(SCHEDULER_LOCK_PATH)
            print("🧹 Removed stale scheduler lock file")
        
        # Remove any job lock files older than 24 hours
        lock_pattern = '/tmp/backup_job_*.lock'
        import glob
        for lock_file in glob.glob(lock_pattern):
            try:
                file_age = time.time() - os.path.getmtime(lock_file)
                if file_age > 86400:  # 24 hours
                    os.unlink(lock_file)
                    print(f"🧹 Removed stale lock file: {lock_file}")
            except:
                pass
    except Exception as e:
        print(f"⚠️ Error cleaning up stale locks: {e}")

def is_lock_file_active(lock_path):
    try:
        with open(lock_path, 'a+') as lock_handle:
            try:
                fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(lock_handle, fcntl.LOCK_UN)
                return False
            except IOError:
                return True
    except OSError:
        return False

# Run cleanup on module import
cleanup_stale_locks()

# Debug function to print current state
def print_scheduler_debug_info():
    """Print debug information about scheduler state"""
    global scheduler
    print("\n" + "="*50)
    print("SCHEDULER DEBUG INFORMATION")
    print("="*50)
    
    if not scheduler:
        print("❌ Scheduler: NOT INITIALIZED")
        return
    
    status = get_scheduler_status()
    print(f"📊 Scheduler Status: {status['status']}")
    print(f"🏃 Running: {status['running']}")
    print(f"📋 Job Count: {status['job_count']}")
    print(f"⏰ Timezone: {scheduler.timezone if hasattr(scheduler, 'timezone') else 'UTC'}")
    
    if status['job_count'] > 0:
        print("\n📅 Scheduled Jobs (Local Time):")
        for job_info in status['next_runs']:
            # Convert to local time
            job = scheduler.get_job(job_info['job_id'])
            if job and job.next_run_time:
                local_time = job.next_run_time.astimezone()
                print(f"   - {job_info['job_id']}: {local_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            else:
                print(f"   - {job_info['job_id']}: Not scheduled")
    else:
        print("\n📭 No jobs scheduled")
    
    print("="*50 + "\n")

# Call debug info when module loads
print_scheduler_debug_info()
