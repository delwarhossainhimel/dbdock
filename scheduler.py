from apscheduler.schedulers.background import BackgroundScheduler
from flask_apscheduler import APScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from models import db, BackupHistory
from backup_scripts.mysql_backup import mysql_backup, apply_retention_policy as mysql_retention
from backup_scripts.postgres_backup import postgres_backup, apply_retention_policy as postgres_retention
import json
from datetime import datetime, timedelta
import atexit
import fcntl
import os
import sys
import time

# Global variable to track if scheduler is already running
scheduler = None
scheduler_lock_file = None

def init_scheduler(app):
    global scheduler, scheduler_lock_file
    
    # Prevent multiple scheduler instances
    if scheduler and scheduler.running:
        print("✅ Scheduler is already running")
        return scheduler
    
    # Create a lock file to prevent multiple instances
    try:
        scheduler_lock_file = open('/tmp/backup_scheduler.lock', 'w')
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
            timezone='UTC'
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
            if os.path.exists('/tmp/backup_scheduler.lock'):
                os.unlink('/tmp/backup_scheduler.lock')
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
    
    # Parse cron expression
    cron_parts = job.cron_expression.split()
    if len(cron_parts) != 5:
        print(f"❌ Invalid cron expression: {job.cron_expression}")
        return
    
    minute, hour, day, month, day_of_week = cron_parts
    
    try:
        # Add new job with proper configuration
        scheduler_obj.add_job(
            id=f'backup_job_{job.id}',
            func=run_backup_job,
            args=[job.id],
            trigger='cron',
            minute=minute,
            hour=hour,
            day=day if day != '*' else None,
            month=month if month != '*' else None,
            day_of_week=day_of_week if day_of_week != '*' else None,
            replace_existing=True,
            coalesce=True,  # Combine multiple pending executions
            max_instances=1,  # Only one instance
            misfire_grace_time=300  # 5 minutes grace period
        )
        print(f"✅ Successfully scheduled job: {job.name} (ID: {job.id})")
        print(f"   📅 Schedule: {job.cron_expression}")
        print(f"   🗃️  Databases: {json.loads(job.databases)}")
        print(f"   ⏰ Next run: {get_next_run_time(scheduler_obj, job.id)}")
        
    except Exception as e:
        print(f"❌ Error scheduling job {job.name}: {e}")

def get_next_run_time(scheduler_obj, job_id):
    """Get the next run time for a job"""
    try:
        job = scheduler_obj.get_job(f'backup_job_{job_id}')
        if job and hasattr(job, 'next_run_time') and job.next_run_time:
            return job.next_run_time.strftime('%Y-%m-%d %H:%M:%S UTC')
        return "Calculating..."
    except Exception as e:
        return f"Error: {str(e)}"

def unschedule_backup_job(scheduler_obj, job_id):
    """Remove a backup job from the scheduler"""
    if not scheduler_obj:
        return
        
    job_id_str = f'backup_job_{job_id}'
    try:
        scheduler_obj.remove_job(job_id_str)
        print(f"✅ Unscheduled job: {job_id_str}")
    except Exception as e:
        # Job might not exist, which is fine
        pass

def run_backup_job(job_id):
    """
    Run backup job with proper locking to prevent multiple executions
    """
    print(f"🔹 Starting backup job ID: {job_id}")
    
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
        # Import app inside the function to avoid circular imports during scheduler initialization
        from app import app
        
        with app.app_context():
            from models import BackupJob, DatabaseServer, StorageLocation, BackupHistory, db
            
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
            
            print(f"🚀 Starting backup: {job.name}")
            
            # Create backup history record
            history = BackupHistory(
                backup_job_id=job.id,
                start_time=datetime.now(),
                status='running'
            )
            db.session.add(history)
            db.session.commit()
            
            try:
                server = job.database_server
                location = job.storage_location
                databases = json.loads(job.databases)
                
                print(f"📊 Backup details:")
                print(f"   - Server: {server.name} ({server.type.value})")
                print(f"   - Storage: {location.name} ({location.type.value})")
                print(f"   - Databases: {databases}")
                print(f"   - Schedule: {job.schedule_type}")
                print(f"   - Folder: {job.folder_path}")
                
                # Run backup based on database type
                if server.type.value == 'mysql':
                    success, message, file_path, file_size = mysql_backup(
                        server, databases, location, job.folder_path, job.schedule_type
                    )
                    retention_func = mysql_retention
                elif server.type.value == 'postgres':
                    success, message, file_path, file_size = postgres_backup(
                        server, databases, location, job.folder_path, job.schedule_type
                    )
                    retention_func = postgres_retention
                else:
                    success, message, file_path, file_size = False, "Unsupported database type", None, 0
                    retention_func = None
                
                # Update history record
                history.end_time = datetime.now()
                history.status = 'success' if success else 'failed'
                history.message = message
                history.file_path = file_path
                history.file_size = file_size
                
                duration = (history.end_time - history.start_time).total_seconds()
                
                if success:
                    print(f"✅ Backup completed successfully: {job.name}")
                    print(f"   - Duration: {duration:.2f} seconds")
                    print(f"   - File size: {file_size} bytes")
                    print(f"   - File path: {file_path}")
                    
                    # Apply retention policy for successful backups
                    if retention_func:
                        try:
                            print("🧹 Applying retention policy...")
                            retention_func(location, job.folder_path, job.schedule_type, [])
                        except Exception as e:
                            print(f"⚠️ Error applying retention policy: {e}")
                    
                else:
                    print(f"❌ Backup failed: {job.name}")
                    print(f"   - Error: {message}")
                    print(f"   - Duration: {duration:.2f} seconds")
                
                # Send notification email if configured
                if job.notification_email:
                    send_notification_email(job, success, message)
                    
            except Exception as e:
                history.end_time = datetime.now()
                history.status = 'failed'
                history.message = f"Unexpected error: {str(e)}"
                print(f"💥 Unexpected error in backup job {job.name}: {e}")
                import traceback
                traceback.print_exc()
            
            db.session.commit()
            
    except Exception as e:
        print(f"💥 Critical error in run_backup_job for job {job_id}: {e}")
        import traceback
        traceback.print_exc()
        
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
            print(f"⚠️ Error releasing lock for job {job_id}: {e}")

def send_notification_email(job, success, message):
    """
    Send email notification about backup status
    """
    try:
        subject = f"Backup {'Completed Successfully' if success else 'Failed'}: {job.name}"
        body = f"""
Backup Job: {job.name}
Status: {'SUCCESS' if success else 'FAILED'}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Message: {message}
        """
        
        # TODO: Implement your email sending logic here
        # This could use SMTP, SendGrid, AWS SES, etc.
        print(f"📧 Email notification would be sent to: {job.notification_email}")
        print(f"   Subject: {subject}")
        print(f"   Body: {body}")
        
    except Exception as e:
        print(f"⚠️ Error sending notification email: {e}")

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
        # Remove scheduler lock file
        if os.path.exists('/tmp/backup_scheduler.lock'):
            os.unlink('/tmp/backup_scheduler.lock')
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
    
    if status['job_count'] > 0:
        print("\n📅 Scheduled Jobs:")
        for job_info in status['next_runs']:
            print(f"   - {job_info['job_id']}: {job_info['next_run']}")
    else:
        print("\n📭 No jobs scheduled")
    
    print("="*50 + "\n")

# Call debug info when module loads
print_scheduler_debug_info()