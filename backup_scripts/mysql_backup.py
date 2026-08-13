import os
import subprocess
import json
from datetime import datetime
from .storage_providers import get_storage_provider
from .utils import create_job_tmp_directory, cleanup_job_tmp_directory, create_full_folder_path
from job_schedules import get_enabled_schedule_types, get_schedule_retention, normalize_schedule_config

# def mysql_backup(server, databases, location, folder_path, schedule_types, job_id=None):
#     """
#     MySQL backup with job-specific temporary directory
#     Supports auto-detection of all databases when 'all' is in the databases list
#     """
#     # Create job-specific tmp directory
#     job_tmp_dir = create_job_tmp_directory(job_id)
    
#     try:
#         print(f"Backup targets: {schedule_types}")
#         print(f"Job temporary directory: {job_tmp_dir}")
        
#         # Check if 'all' is selected - auto-detect databases
#         if 'all' in databases or '__all__' in databases:
#             print("🔄 Auto-detecting all databases from server...")
#             databases = get_all_mysql_databases(server)
#             if not databases:
#                 return False, "No databases found on the server", None, 0, []
#             print(f"✅ Auto-detected {len(databases)} databases: {', '.join(databases)}")
#         else:
#             print(f"📊 Using manually selected databases: {', '.join(databases)}")
        
#         backup_files = []
#         database_results = []
#         total_success = 0
#         total_failed = 0
        
#         for database in databases:
#             # Generate filename with new format: database_YYYY-MM-DD.sql.gz
#             timestamp = datetime.now().strftime('%Y-%m-%d')
#             filename = f"{database}_{timestamp}.sql.gz"
#             filepath = os.path.join(job_tmp_dir, filename)
            
#             print(f"Creating MySQL backup: {filepath}")
            
#             # Run mysqldump command
#             cmd = [
#                 'mysqldump',
#                 f'-h{server.host}',
#                 f'-P{server.port}',
#                 f'-u{server.username}',
#                 f'-p{server.password}',
#                 '--single-transaction',
#                 '--routines',
#                 '--triggers',
#                 database
#             ]
            
#             print(f"Running command: mysqldump -h{server.host} -P{server.port} -u{server.username} [database: {database}]")
            
#             # Execute dump and compress
#             try:
#                 with open(filepath, 'w') as f:
#                     dump_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#                     compress_process = subprocess.Popen(['gzip'], stdin=dump_process.stdout, stdout=f)
#                     compress_process.wait()
                
#                 # Check if the process was successful
#                 if compress_process.returncode == 0:
#                     # Verify file was created
#                     if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
#                         backup_files.append((filepath, filename))
#                         file_size = os.path.getsize(filepath)
#                         print(f"✓ Created MySQL backup: {filepath} ({file_size} bytes)")
#                         database_results.append({
#                             'database': database,
#                             'status': 'success',
#                             'message': 'Backup completed successfully',
#                             'file_size': file_size,
#                         })
#                         total_success += 1
#                     else:
#                         print(f"✗ Backup file was not created properly for database {database}")
#                         database_results.append({
#                             'database': database,
#                             'status': 'failed',
#                             'message': f"Backup file was not created properly for database {database}",
#                             'file_size': 0,
#                         })
#                         total_failed += 1
#                 else:
#                     # Get error message
#                     _, stderr = dump_process.communicate()
#                     error_msg = stderr.decode() if stderr else "Unknown error"
#                     print(f"✗ MySQL backup failed for database {database}: {error_msg}")
#                     database_results.append({
#                         'database': database,
#                         'status': 'failed',
#                         'message': f"MySQL backup failed for database {database}: {error_msg}",
#                         'file_size': 0,
#                     })
#                     total_failed += 1
                    
#             except Exception as e:
#                 print(f"✗ Error during MySQL backup process for database {database}: {str(e)}")
#                 database_results.append({
#                     'database': database,
#                     'status': 'failed',
#                     'message': f"Error during MySQL backup process for database {database}: {str(e)}",
#                     'file_size': 0,
#                 })
#                 total_failed += 1

#         if backup_files:
#             result = upload_backups_for_schedules(location, folder_path, schedule_types, backup_files)
#         else:
#             result = (False, "No MySQL backup files were created", None, 0)
        
#         # Clean up only this job's temporary directory
#         cleanup_job_tmp_directory(job_tmp_dir)

#         # Determine overall success based on ALL databases
#         # If any database failed, the overall backup is considered PARTIAL or FAILED
#         if total_failed == 0 and total_success > 0:
#             # All databases succeeded
#             message = f"Successfully backed up all {total_success} databases"
#             return True, message, result[2], result[3], database_results
#         elif total_success > 0 and total_failed > 0:
#             # Partial success - some databases failed
#             message = f"Partial success: {total_success} succeeded, {total_failed} failed out of {len(databases)} databases"
#             return False, message, result[2], result[3], database_results
#         elif total_success == 0 and total_failed > 0:
#             # All databases failed
#             message = f"All {total_failed} databases failed to backup"
#             return False, message, None, 0, database_results
#         else:
#             # No databases processed
#             return False, "No databases were processed", None, 0, database_results
            
#     except Exception as e:
#         # Clean up on error too - but only this job's directory
#         cleanup_job_tmp_directory(job_tmp_dir)
#         return False, str(e), None, 0, []
def mysql_backup(server, databases, location, folder_path, schedule_types, job_id=None):
    """
    MySQL backup with job-specific temporary directory
    Supports auto-detection of all databases when 'all' is in the databases list
    """
    # Create job-specific tmp directory
    job_tmp_dir = create_job_tmp_directory(job_id)
    
    try:
        print(f"Backup targets: {schedule_types}")
        print(f"Job temporary directory: {job_tmp_dir}")
        
        # Check if 'all' is selected - auto-detect databases
        if 'all' in databases or '__all__' in databases:
            print("🔄 Auto-detecting all databases from server...")
            databases = get_all_mysql_databases(server)
            if not databases:
                return False, "No databases found on the server", None, 0, []
            print(f"✅ Auto-detected {len(databases)} databases: {', '.join(databases)}")
        else:
            print(f"📊 Using manually selected databases: {', '.join(databases)}")
        
        backup_files = []
        database_results = []
        total_success = 0
        total_failed = 0
        
        # First, verify all databases exist
        print("🔍 Verifying databases exist...")
        for database in databases:
            if not check_database_exists(server, database):
                print(f"✗ Database '{database}' does NOT exist on the server!")
                database_results.append({
                    'database': database,
                    'status': 'failed',
                    'message': f"Database '{database}' does not exist on the server",
                    'file_size': 0,
                })
                total_failed += 1
        
        # Filter out non-existent databases
        valid_databases = [db for db in databases if check_database_exists(server, db)]
        if not valid_databases:
            return False, "No valid databases found to backup", None, 0, database_results
        
        print(f"✅ Found {len(valid_databases)} valid databases: {', '.join(valid_databases)}")
        
        for database in valid_databases:
            # Generate filename with new format: database_YYYY-MM-DD.sql.gz
            timestamp = datetime.now().strftime('%Y-%m-%d')
            filename = f"{database}_{timestamp}.sql.gz"
            filepath = os.path.join(job_tmp_dir, filename)
            
            print(f"Creating MySQL backup: {filepath}")
            
            # Run mysqldump command
            cmd = [
                'mysqldump',
                f'-h{server.host}',
                f'-P{server.port}',
                f'-u{server.username}',
                f'-p{server.password}',
                '--single-transaction',
                '--routines',
                '--triggers',
                database
            ]
            
            print(f"Running command: mysqldump -h{server.host} -P{server.port} -u{server.username} [database: {database}]")
            
            # Execute dump with proper error capture
            try:
                # Use Popen with pipe for stderr capture
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                # Open the output file for writing (will be compressed with gzip)
                with open(filepath, 'w') as f:
                    # Compress the output with gzip
                    gzip_process = subprocess.Popen(
                        ['gzip'],
                        stdin=process.stdout,
                        stdout=f
                    )
                    process.stdout.close()
                    gzip_process.communicate()
                
                # Check if the gzip process was successful
                if gzip_process.returncode == 0:
                    # Verify file was created and has content
                    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                        backup_files.append((filepath, filename))
                        file_size = os.path.getsize(filepath)
                        print(f"✓ Created MySQL backup: {filepath} ({file_size} bytes)")
                        database_results.append({
                            'database': database,
                            'status': 'success',
                            'message': 'Backup completed successfully',
                            'file_size': file_size,
                        })
                        total_success += 1
                    else:
                        print(f"✗ Backup file was not created properly for database {database}")
                        database_results.append({
                            'database': database,
                            'status': 'failed',
                            'message': f"Backup file was not created properly for database {database}",
                            'file_size': 0,
                        })
                        total_failed += 1
                else:
                    # Get error message from process
                    _, stderr = process.communicate()
                    error_msg = stderr.strip() if stderr else "Unknown error"
                    print(f"✗ MySQL backup failed for database {database}: {error_msg}")
                    database_results.append({
                        'database': database,
                        'status': 'failed',
                        'message': f"MySQL backup failed: {error_msg}",
                        'file_size': 0,
                    })
                    total_failed += 1
                    
            except Exception as e:
                print(f"✗ Error during MySQL backup process for database {database}: {str(e)}")
                database_results.append({
                    'database': database,
                    'status': 'failed',
                    'message': f"Error during backup: {str(e)}",
                    'file_size': 0,
                })
                total_failed += 1

        if backup_files:
            result = upload_backups_for_schedules(location, folder_path, schedule_types, backup_files)
        else:
            result = (False, "No MySQL backup files were created", None, 0)
        
        # Clean up only this job's temporary directory
        cleanup_job_tmp_directory(job_tmp_dir)

        # Determine overall success based on ALL databases
        if total_failed == 0 and total_success > 0:
            message = f"Successfully backed up all {total_success} databases"
            return True, message, result[2], result[3], database_results
        elif total_success > 0 and total_failed > 0:
            message = f"Partial success: {total_success} succeeded, {total_failed} failed out of {len(databases)} databases"
            return False, message, result[2], result[3], database_results
        elif total_success == 0 and total_failed > 0:
            message = f"All {total_failed} databases failed to backup"
            return False, message, None, 0, database_results
        else:
            return False, "No databases were processed", None, 0, database_results
            
    except Exception as e:
        cleanup_job_tmp_directory(job_tmp_dir)
        return False, str(e), None, 0, []


def check_database_exists(server, database):
    """
    Check if a database exists on the MySQL server
    """
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=server.host,
            port=server.port,
            user=server.username,
            password=server.password
        )
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES LIKE %s", (database,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result is not None
    except Exception as e:
        print(f"⚠️ Error checking database '{database}': {e}")
        return False


def get_all_mysql_databases(server):
    """
    Connect to MySQL server and get all databases excluding system databases
    """
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=server.host,
            port=server.port,
            user=server.username,
            password=server.password
        )
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        
        # Exclude system databases
        excluded = ['information_schema', 'mysql', 'performance_schema', 'sys']
        databases = [db[0] for db in cursor.fetchall() if db[0] not in excluded]
        
        cursor.close()
        conn.close()
        
        return databases
    except Exception as e:
        print(f"⚠️ Error detecting databases from MySQL server: {e}")
        return []

def get_all_mysql_databases(server):
    """
    Connect to MySQL server and get all databases excluding system databases
    """
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=server.host,
            port=server.port,
            user=server.username,
            password=server.password
        )
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        
        # Exclude system databases
        excluded = ['information_schema', 'mysql', 'performance_schema', 'sys']
        databases = [db[0] for db in cursor.fetchall() if db[0] not in excluded]
        
        cursor.close()
        conn.close()
        
        return databases
    except Exception as e:
        print(f"⚠️ Error detecting databases from MySQL server: {e}")
        return []


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


def upload_backups_for_schedules(location, base_folder_path, schedule_types, backup_files):
    uploaded_paths = []
    total_size = 0

    for schedule_type in schedule_types:
        full_folder_path = create_full_folder_path(base_folder_path, schedule_type)
        print(f"Full backup path: {full_folder_path}")

        result = upload_to_storage(location, full_folder_path, backup_files)
        if not result[0]:
            return result

        uploaded_paths.extend([path for path in (result[2] or "").split(";") if path])
        total_size += result[3] or 0
        apply_retention_policy(location, base_folder_path, schedule_type)

    return True, "Backup completed successfully", ";".join(uploaded_paths), total_size


def build_partial_failure_message(database_results):
    succeeded = len([item for item in database_results if item['status'] == 'success'])
    failed = len(database_results) - succeeded
    return f"Backup completed with partial failures. {succeeded} succeeded, {failed} failed."


def apply_retention_policy(location, base_folder_path, schedule_type):
    """
    Apply retention policy by deleting old backup files
    """
    try:
        from app import app
        from models import BackupJob
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
                
                job_schedule_types = get_enabled_schedule_types(
                    normalize_schedule_config(
                        job.schedule_config,
                        job.schedule_type,
                        job.cron_expression,
                        fallback_retention=job.retention_policy,
                    )
                )

                if schedule_type not in job_schedule_types:
                    continue

                retention_value = get_schedule_retention(
                    job.schedule_config,
                    schedule_type,
                    fallback_retention=job.retention_policy or 1,
                )

                if schedule_type == 'daily':
                    cutoff_date = datetime.now() - timedelta(days=retention_value)
                elif schedule_type == 'weekly':
                    cutoff_date = datetime.now() - timedelta(weeks=retention_value)
                elif schedule_type == 'monthly':
                    cutoff_date = datetime.now() - timedelta(days=30 * retention_value)
                elif schedule_type == 'yearly':
                    cutoff_date = datetime.now() - timedelta(days=365 * retention_value)
                else:
                    continue
                
                print(f"Cutoff date: {cutoff_date}, Retention: {retention_value} {schedule_type}(s)")
                
                # Get databases for this job
                databases = json.loads(job.databases)
                
                # Handle 'all' databases - we need to get actual database names
                if 'all' in databases or '__all__' in databases:
                    # For retention policy with auto-detected databases, we need to get the actual list
                    try:
                        # Get the server from the job
                        server = job.database_server
                        if server.type.value == 'mysql':
                            actual_databases = get_all_mysql_databases(server)
                            if actual_databases:
                                databases = actual_databases
                    except Exception as e:
                        print(f"⚠️ Could not auto-detect databases for retention: {e}")
                        continue
                
                # Delete old files for each database from the schedule-specific folder
                for database in databases:
                    deleted_count = delete_old_backup_files(location, full_folder_path, database, cutoff_date)
                    if deleted_count > 0:
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