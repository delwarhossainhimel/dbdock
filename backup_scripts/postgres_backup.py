import os
import subprocess
import json
from datetime import datetime
from .storage_providers import get_storage_provider
from .utils import create_job_tmp_directory, cleanup_job_tmp_directory, create_full_folder_path

def postgres_backup(server, databases, location, folder_path, schedule_type, job_id=None):
    """
    PostgreSQL backup with job-specific temporary directory
    """
    # Create job-specific tmp directory
    job_tmp_dir = create_job_tmp_directory(job_id)
    
    try:
        # Create the full folder path with schedule type as subfolder
        full_folder_path = create_full_folder_path(folder_path, schedule_type)
        print(f"Full backup path: {full_folder_path}")
        print(f"Job temporary directory: {job_tmp_dir}")
        
        backup_files = []
        
        for database in databases:
            # Generate filename with new format: database_YYYY-MM-DD.sql.gz
            timestamp = datetime.now().strftime('%Y-%m-%d')
            filename = f"{database}_{timestamp}.sql.gz"
            filepath = os.path.join(job_tmp_dir, filename)
            
            print(f"Creating backup file: {filepath}")
            
            # Set environment variables for pg_dump
            env = os.environ.copy()
            env['PGPASSWORD'] = server.password
            
            # Run pg_dump command with plain format piped to gzip
            pg_dump_cmd = [
                'pg_dump',
                '-h', server.host,
                '-p', str(server.port),
                '-U', server.username,
                '-F', 'p',  # Plain format
                database
            ]
            
            print(f"Running command: {' '.join(pg_dump_cmd)}")
            
            # Gzip command
            gzip_cmd = ['gzip', '-c']
            
            try:
                # Execute pg_dump and pipe to gzip
                pg_dump_process = subprocess.Popen(pg_dump_cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                # Open the output file for writing
                with open(filepath, 'wb') as outfile:
                    gzip_process = subprocess.Popen(gzip_cmd, stdin=pg_dump_process.stdout, stdout=outfile)
                
                # Wait for processes to complete
                pg_dump_process.stdout.close()
                gzip_process.communicate()
                
                # Check if pg_dump was successful
                pg_dump_exit_code = pg_dump_process.wait()
                
                if pg_dump_exit_code == 0:
                    # Verify file was created
                    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                        backup_files.append((filepath, filename))
                        print(f"✓ Created PostgreSQL backup: {filepath} ({os.path.getsize(filepath)} bytes)")
                    else:
                        return False, f"Backup file was not created properly for database {database}", None, 0
                else:
                    # Get error message from pg_dump
                    _, stderr = pg_dump_process.communicate()
                    error_msg = stderr.decode() if stderr else "Unknown error"
                    return False, f"PostgreSQL backup failed for database {database}: {error_msg}", None, 0
                    
            except Exception as e:
                return False, f"Error during backup process for database {database}: {str(e)}", None, 0
        
        if not backup_files:
            return False, "No backup files were created", None, 0
        
        # Upload to storage location using the storage provider
        result = upload_to_storage(location, full_folder_path, backup_files)
        
        # Apply retention policy after successful backup
        if result[0]:  # If backup was successful
            apply_retention_policy(location, folder_path, schedule_type, backup_files)
        
        # Clean up only this job's temporary directory
        cleanup_job_tmp_directory(job_tmp_dir)
        
        return result
            
    except Exception as e:
        # Clean up on error too - but only this job's directory
        cleanup_job_tmp_directory(job_tmp_dir)
        return False, str(e), None, 0

def upload_to_storage(location, folder_path, backup_files):
    """Upload backup files using the appropriate storage provider"""
    config = json.loads(location.config)
    storage_type = location.type.value
    
    # Get the appropriate storage provider
    storage_provider = get_storage_provider(storage_type)
    if not storage_provider:
        return False, f"Unsupported storage type: {storage_type}", None, 0
    
    try:
        return storage_provider.upload_files(config, folder_path, backup_files)
    except Exception as e:
        return False, str(e), None, 0

def apply_retention_policy(location, base_folder_path, schedule_type, current_backup_files):
    """
    Apply retention policy by deleting old backup files
    """
    try:
        from app import app
        from models import BackupJob, BackupHistory
        from datetime import datetime, timedelta
        
        # Create the full folder path for retention policy checks
        full_folder_path = create_full_folder_path(base_folder_path, schedule_type)
        
        with app.app_context():
            # Get all backup jobs that use this storage location and base folder
            jobs = BackupJob.query.filter_by(
                storage_location_id=location.id,
                folder_path=base_folder_path,
                is_active=True
            ).all()
            
            for job in jobs:
                print(f"Applying retention policy for job: {job.name}")
                
                # Calculate cutoff date based on retention policy and schedule type
                if job.schedule_type == 'daily':
                    cutoff_date = datetime.now() - timedelta(days=job.retention_policy)
                elif job.schedule_type == 'weekly':
                    cutoff_date = datetime.now() - timedelta(weeks=job.retention_policy)
                elif job.schedule_type == 'monthly':
                    cutoff_date = datetime.now() - timedelta(days=30 * job.retention_policy)
                else:
                    continue
                
                print(f"Cutoff date: {cutoff_date}, Retention: {job.retention_policy} {job.schedule_type}(s)")
                
                # Get databases for this job
                databases = json.loads(job.databases)
                
                # Delete old files for each database from the schedule-specific folder
                for database in databases:
                    deleted_count = delete_old_backup_files(location, full_folder_path, database, cutoff_date)
                    print(f"Deleted {deleted_count} old backup files for database: {database}")
                
    except Exception as e:
        print(f"Error applying retention policy: {e}")

def delete_old_backup_files(location, folder_path, database, cutoff_date):
    """Delete old backup files using the appropriate storage provider"""
    config = json.loads(location.config)
    storage_type = location.type.value
    
    # Get the appropriate storage provider
    storage_provider = get_storage_provider(storage_type)
    if not storage_provider:
        print(f"Unsupported storage type for deletion: {storage_type}")
        return 0
    
    try:
        return storage_provider.delete_old_files(config, folder_path, database, cutoff_date)
    except Exception as e:
        print(f"Error deleting old backup files: {e}")
        return 0