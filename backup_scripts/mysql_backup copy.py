import os
import subprocess
import json
from datetime import datetime
import shutil
import glob
import ftplib
import socket
import uuid

def mysql_backup(server, databases, location, folder_path, schedule_type, job_id=None):
    """
    MySQL backup with job-specific temporary directory
    """
    # Create job-specific tmp directory
    job_tmp_dir = create_job_tmp_directory(job_id)
    
    try:
        # Clean up old temporary files for this job (older than 24 hours)
        cleanup_old_job_tmp_files(job_tmp_dir, max_age_hours=24)
        
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
                '--skip-lock-tables',
                '--skip-add-locks',
                database
            ]
            
            print(f"Running command: mysqldump -h{server.host} -P{server.port} -u{server.username} [database: {database}]")
            
            # Execute dump and compress
            try:
                with open(filepath, 'w') as f:
                    dump_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    compress_process = subprocess.Popen(['gzip'], stdin=dump_process.stdout, stdout=f)
                    compress_process.wait()
                
                # Check if the process was successful
                if compress_process.returncode == 0:
                    # Verify file was created
                    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                        backup_files.append((filepath, filename))
                        file_size = os.path.getsize(filepath)
                        print(f"✓ Created MySQL backup: {filepath} ({file_size} bytes)")
                    else:
                        return False, f"Backup file was not created properly for database {database}", None, 0
                else:
                    # Get error message
                    _, stderr = dump_process.communicate()
                    error_msg = stderr.decode() if stderr else "Unknown error"
                    return False, f"MySQL backup failed for database {database}: {error_msg}", None, 0
                    
            except Exception as e:
                return False, f"Error during MySQL backup process for database {database}: {str(e)}", None, 0
        
        if not backup_files:
            return False, "No MySQL backup files were created", None, 0
        
        # Upload to storage location using the full folder path
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

def create_job_tmp_directory(job_id=None):
    """
    Create a job-specific temporary directory
    """
    base_tmp_dir = os.path.join(os.getcwd(), 'tmp')
    
    # Create base tmp directory if it doesn't exist
    os.makedirs(base_tmp_dir, exist_ok=True)
    
    # Create job-specific directory
    if job_id:
        job_dir = os.path.join(base_tmp_dir, f"job_{job_id}")
    else:
        # Use timestamp and random string for unique directory
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        random_str = str(uuid.uuid4())[:8]
        job_dir = os.path.join(base_tmp_dir, f"job_{timestamp}_{random_str}")
    
    os.makedirs(job_dir, exist_ok=True)
    return job_dir

def cleanup_job_tmp_directory(job_tmp_dir):
    """
    Clean up only the specific job's temporary directory
    """
    if not os.path.exists(job_tmp_dir):
        return True
        
    try:
        # Delete all files and subdirectories in the job's tmp directory
        for item in os.listdir(job_tmp_dir):
            item_path = os.path.join(job_tmp_dir, item)
            
            # Skip if it's the directory itself or special files
            if item in ['.', '..']:
                continue
                
            try:
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)
                    print(f"Deleted job temporary file: {item_path}")
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                    print(f"Deleted job temporary directory: {item_path}")
            except Exception as e:
                print(f"Failed to delete {item_path}. Reason: {e}")
        
        # Remove the job directory itself
        try:
            os.rmdir(job_tmp_dir)
            print(f"Cleaned up job temporary directory: {job_tmp_dir}")
        except OSError:
            # Directory might not be empty, but we tried our best
            print(f"Note: Could not remove directory {job_tmp_dir} (may not be empty)")
                
        return True
    except Exception as e:
        print(f"Error cleaning up job tmp directory {job_tmp_dir}: {e}")
        return False

def cleanup_old_job_tmp_files(job_tmp_dir, max_age_hours=24):
    """
    Clean up temporary files older than specified hours in job directory
    """
    if not os.path.exists(job_tmp_dir):
        return
        
    current_time = datetime.now().timestamp()
    max_age_seconds = max_age_hours * 3600
    
    try:
        for filename in os.listdir(job_tmp_dir):
            file_path = os.path.join(job_tmp_dir, filename)
            
            # Skip if it's the directory itself or special files
            if filename in ['.', '..']:
                continue
                
            try:
                # Get file modification time
                file_mtime = os.path.getmtime(file_path)
                file_age = current_time - file_mtime
                
                if file_age > max_age_seconds:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                        print(f"Deleted old job temporary file: {file_path} (age: {file_age/3600:.1f} hours)")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        print(f"Deleted old job temporary directory: {file_path} (age: {file_age/3600:.1f} hours)")
            except Exception as e:
                print(f"Failed to delete old file {file_path}. Reason: {e}")
                
    except Exception as e:
        print(f"Error cleaning up old job tmp files in {job_tmp_dir}: {e}")

def cleanup_global_old_tmp_files(max_age_hours=24):
    """
    Clean up old job directories from the main tmp folder
    This should be run separately (e.g., daily cleanup job)
    """
    base_tmp_dir = os.path.join(os.getcwd(), 'tmp')
    if not os.path.exists(base_tmp_dir):
        return
        
    current_time = datetime.now().timestamp()
    max_age_seconds = max_age_hours * 3600
    
    try:
        for item in os.listdir(base_tmp_dir):
            item_path = os.path.join(base_tmp_dir, item)
            
            # Skip if it's the directory itself or special files
            if item in ['.', '..']:
                continue
            
            # Only clean up job directories (those starting with "job_")
            if item.startswith('job_'):
                try:
                    # Get directory modification time
                    item_mtime = os.path.getmtime(item_path)
                    item_age = current_time - item_mtime
                    
                    if item_age > max_age_seconds:
                        if os.path.isdir(item_path):
                            shutil.rmtree(item_path)
                            print(f"Deleted old job directory: {item_path} (age: {item_age/3600:.1f} hours)")
                except Exception as e:
                    print(f"Failed to delete old job directory {item_path}. Reason: {e}")
                    
    except Exception as e:
        print(f"Error cleaning up global tmp files: {e}")

# The rest of the functions remain the same as before...
def create_full_folder_path(base_folder_path, schedule_type):
    """
    Create the full folder path by appending schedule type as subfolder
    Example: 
    - base_folder_path: "test", schedule_type: "daily" -> "test/Daily"
    - base_folder_path: "backups/prod", schedule_type: "weekly" -> "backups/prod/Weekly"
    """
    # Capitalize the first letter of schedule type
    schedule_folder = schedule_type.capitalize()
    
    # Combine base path with schedule folder
    if base_folder_path:
        full_path = f"{base_folder_path}/{schedule_folder}"
    else:
        full_path = schedule_folder
    
    return full_path

def upload_to_storage(location, folder_path, backup_files):
    config = json.loads(location.config)
    
    try:
        if location.type.value == 'local':
            return upload_to_local(config, folder_path, backup_files)
        elif location.type.value == 'ftp':
            return upload_to_ftp(config, folder_path, backup_files)
        elif location.type.value == 's3':
            return upload_to_s3(config, folder_path, backup_files)
        elif location.type.value == 'blob':
            return upload_to_blob(config, folder_path, backup_files)
        else:
            return False, "Unsupported storage type", None, 0
    except Exception as e:
        return False, str(e), None, 0

def upload_to_local(config, folder_path, backup_files):
    total_size = 0
    uploaded_files = []
    
    try:
        target_dir = os.path.join(config['path'], folder_path)
        os.makedirs(target_dir, exist_ok=True)
        
        for source_path, filename in backup_files:
            target_path = os.path.join(target_dir, filename)
            
            # Copy file
            shutil.copy2(source_path, target_path)
            
            # Get file size
            file_size = os.path.getsize(target_path)
            total_size += file_size
            uploaded_files.append(target_path)
            print(f"Uploaded to local storage: {target_path}")
        
        return True, "Backup completed successfully", ";".join(uploaded_files), total_size
    except Exception as e:
        return False, str(e), None, 0

def upload_to_ftp(config, folder_path, backup_files):
    """
    Upload backup files to FTP server
    """
    ftp = None
    try:
        # Extract FTP configuration
        host = config.get('host', '')
        port = int(config.get('port', 21))
        username = config.get('username', '')
        password = config.get('password', '')
        passive_mode = config.get('passive_mode', True)
        
        if not host:
            return False, "FTP host not configured", None, 0
        
        print(f"Connecting to FTP server: {host}:{port}")
        
        # Create FTP connection
        ftp = ftplib.FTP()
        ftp.connect(host, port, timeout=30)
        ftp.login(username, password)
        
        # Set passive mode if configured
        if passive_mode:
            ftp.set_pasv(True)
        
        # Create remote directory structure
        remote_path = folder_path.strip('/')
        create_ftp_directory(ftp, remote_path)
        
        total_size = 0
        uploaded_files = []
        
        # Upload each backup file
        for local_path, filename in backup_files:
            remote_file_path = f"{remote_path}/{filename}" if remote_path else filename
            
            print(f"Uploading {filename} to FTP...")
            
            # Upload file in binary mode
            with open(local_path, 'rb') as file_obj:
                ftp.storbinary(f'STOR {remote_file_path}', file_obj)
            
            # Get file size
            file_size = os.path.getsize(local_path)
            total_size += file_size
            uploaded_files.append(f"ftp://{host}/{remote_file_path}")
            
            print(f"Successfully uploaded: {filename} ({file_size} bytes)")
        
        # Close FTP connection
        ftp.quit()
        
        return True, "FTP upload completed successfully", ";".join(uploaded_files), total_size
        
    except ftplib.all_errors as e:
        error_msg = f"FTP error: {str(e)}"
        print(error_msg)
        return False, error_msg, None, 0
    except socket.gaierror as e:
        error_msg = f"FTP connection error: {str(e)}"
        print(error_msg)
        return False, error_msg, None, 0
    except Exception as e:
        error_msg = f"FTP upload failed: {str(e)}"
        print(error_msg)
        return False, error_msg, None, 0
    finally:
        # Ensure FTP connection is closed
        if ftp:
            try:
                ftp.close()
            except:
                pass

def create_ftp_directory(ftp, remote_path):
    """
    Create directory structure on FTP server recursively with better error handling
    """
    if not remote_path or remote_path == '/':
        return
    
    # Normalize path - remove leading/trailing slashes and split
    normalized_path = remote_path.strip('/')
    if not normalized_path:
        return
    
    path_parts = [part for part in normalized_path.split('/') if part]
    
    # Save current directory
    original_dir = ftp.pwd()
    
    try:
        # Start from root directory
        ftp.cwd("/")
        
        for part in path_parts:
            print(f"Processing directory: {part}")
            
            try:
                # Try to change to the directory
                ftp.cwd(part)
                print(f"✓ Directory exists: {part}")
            except ftplib.error_perm as e:
                error_msg = str(e)
                print(f"Directory doesn't exist or access denied: {part}, error: {error_msg}")
                
                # Try to create the directory
                try:
                    ftp.mkd(part)
                    print(f"✓ Created directory: {part}")
                    
                    # Try to enter the newly created directory
                    try:
                        ftp.cwd(part)
                        print(f"✓ Entered directory: {part}")
                    except ftplib.error_perm as enter_error:
                        print(f"✗ Cannot enter directory {part} after creation: {enter_error}")
                        # Continue anyway, as the directory was created
                        
                except ftplib.error_perm as mkdir_error:
                    mkdir_error_msg = str(mkdir_error)
                    if "550" in mkdir_error_msg and "exists" in mkdir_error_msg.lower():
                        print(f"✓ Directory already exists (despite error): {part}")
                        # Try to enter it again
                        try:
                            ftp.cwd(part)
                        except:
                            pass
                    elif "550" in mkdir_error_msg and "permission" in mkdir_error_msg.lower():
                        print(f"✗ Permission denied creating directory: {part}")
                        raise PermissionError(f"FTP permission denied for directory: {part}")
                    else:
                        print(f"✗ Failed to create directory {part}: {mkdir_error}")
                        raise mkdir_error
            
            except Exception as e:
                print(f"✗ Unexpected error with directory {part}: {e}")
                raise e
        
        print(f"✓ Successfully created/verified directory structure: {remote_path}")
        
    except Exception as e:
        print(f"✗ Failed to create FTP directory structure: {e}")
        raise e
    finally:
        # Always return to original directory
        try:
            ftp.cwd(original_dir)
        except:
            try:
                ftp.cwd("/")
            except:
                pass

def upload_to_blob(config, folder_path, backup_files):
    """
    Upload backup files to Azure Blob Storage using SAS URL
    """
    try:
        # Extract Azure Blob Storage configuration
        container_sas_url = config.get('connection_string', '')
        container_name = config.get('container', '')
        
        # Debug output
        print(f"DEBUG - Container SAS URL: {container_sas_url.split('?')[0]}")  # Print without SAS token
        print(f"DEBUG - Container Name: '{container_name}'")
        
        if not container_sas_url:
            return False, "Azure Blob Storage SAS URL not configured", None, 0
        
        print(f"Connecting to Azure Blob Storage using container SAS URL")
        
        # Import Azure Blob Storage libraries
        try:
            from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
            from azure.core.exceptions import ResourceExistsError
            from urllib.parse import urlparse
        except ImportError:
            return False, "Azure Blob Storage libraries not installed. Please install azure-storage-blob", None, 0
        
        # Extract storage account name from SAS URL dynamically
        try:
            parsed_url = urlparse(container_sas_url)
            # The hostname will be like: apsiswebapplication.blob.core.windows.net
            storage_account_name = parsed_url.hostname.split('.')[0]
            print(f"DEBUG - Extracted Storage Account: {storage_account_name}")
        except Exception as e:
            error_msg = f"Failed to parse storage account name from SAS URL: {str(e)}"
            print(error_msg)
            return False, error_msg, None, 0
        
        total_size = 0
        uploaded_files = []
        
        # Create ContainerClient from the container SAS URL
        try:
            container_client = ContainerClient.from_container_url(container_sas_url)
            # Test the connection by listing blobs (limited to 1 for efficiency)
            list(container_client.list_blobs(maxresults=1))
            print("✓ Successfully connected to Azure Blob Storage container")
        except Exception as e:
            error_msg = f"Failed to connect to Azure Blob Storage container: {str(e)}"
            print(error_msg)
            return False, error_msg, None, 0
        
        # Upload each backup file
        for local_path, filename in backup_files:
            # Create blob path with folder structure
            if folder_path:
                blob_path = f"{folder_path}/{filename}"
            else:
                blob_path = filename
            
            print(f"Uploading {filename} to Azure Blob Storage as {blob_path}...")
            
            try:
                # Get blob client for the specific blob
                blob_client = container_client.get_blob_client(blob_path)
                
                # Upload file
                with open(local_path, "rb") as data:
                    blob_client.upload_blob(
                        data,
                        overwrite=True
                    )
                
                # Get file size
                file_size = os.path.getsize(local_path)
                total_size += file_size
                
                # Construct the readable URL dynamically using extracted storage account name
                storage_url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{blob_path}"
                uploaded_files.append(storage_url)
                
                print(f"✓ Successfully uploaded to Azure Blob Storage: {blob_path} ({file_size} bytes)")
                
            except Exception as e:
                error_msg = f"Failed to upload {filename} to Azure Blob Storage: {str(e)}"
                print(error_msg)
                return False, error_msg, None, 0
        
        return True, "Azure Blob Storage upload completed successfully", ";".join(uploaded_files), total_size
        
    except Exception as e:
        error_msg = f"Azure Blob Storage upload failed: {str(e)}"
        print(error_msg)
        return False, error_msg, None, 0

def upload_to_s3(config, folder_path, backup_files):
    # S3 storage not implemented
    return False, "S3 storage not yet implemented", None, 0

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
                folder_path=base_folder_path,  # Use base folder path (without schedule type)
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
                
                # Also clean up backup history records
                old_history = BackupHistory.query.filter(
                    BackupHistory.backup_job_id == job.id,
                    BackupHistory.start_time < cutoff_date
                ).all()
                
                for history in old_history:
                    db.session.delete(history)
                
                db.session.commit()
                print(f"Cleaned up {len(old_history)} old history records")
                
    except Exception as e:
        print(f"Error applying retention policy: {e}")

def delete_old_backup_files(location, folder_path, database, cutoff_date):
    """
    Delete backup files older than cutoff date
    """
    deleted_count = 0
    
    try:
        if location.type.value == 'local':
            deleted_count = delete_old_local_files(location, folder_path, database, cutoff_date)
        elif location.type.value == 'ftp':
            deleted_count = delete_old_ftp_files(location, folder_path, database, cutoff_date)
        elif location.type.value == 'blob':
            deleted_count = delete_old_blob_files(location, folder_path, database, cutoff_date)
        # Add other storage types as needed
        
    except Exception as e:
        print(f"Error deleting old backup files: {e}")
    
    return deleted_count

def delete_old_local_files(location, folder_path, database, cutoff_date):
    """
    Delete old backup files from local storage
    """
    deleted_count = 0
    try:
        config = json.loads(location.config)
        target_dir = os.path.join(config['path'], folder_path)
        
        if not os.path.exists(target_dir):
            return 0
        
        # Pattern to match backup files for this database
        pattern = f"{database}_*.sql.gz"
        file_pattern = os.path.join(target_dir, pattern)
        
        # Get all matching files
        import glob
        backup_files = glob.glob(file_pattern)
        
        for file_path in backup_files:
            try:
                # Extract date from filename (format: database_YYYY-MM-DD.sql.gz)
                filename = os.path.basename(file_path)
                date_str = filename.replace(f"{database}_", "").replace(".sql.gz", "")
                
                # Parse the date from filename
                file_date = datetime.strptime(date_str, '%Y-%m-%d')
                
                # Check if file is older than cutoff date
                if file_date < cutoff_date:
                    os.remove(file_path)
                    print(f"Deleted old backup file: {file_path}")
                    deleted_count += 1
                    
            except ValueError:
                # Skip files that don't match the expected format
                continue
            except Exception as e:
                print(f"Error deleting file {file_path}: {e}")
                
    except Exception as e:
        print(f"Error in delete_old_local_files: {e}")
    
    return deleted_count

def delete_old_ftp_files(location, folder_path, database, cutoff_date):
    """
    Delete old backup files from FTP storage
    """
    deleted_count = 0
    ftp = None
    
    try:
        config = json.loads(location.config)
        host = config.get('host', '')
        port = int(config.get('port', 21))
        username = config.get('username', '')
        password = config.get('password', '')
        passive_mode = config.get('passive_mode', True)
        
        if not host:
            return 0
        
        # Connect to FTP
        ftp = ftplib.FTP()
        ftp.connect(host, port, timeout=30)
        ftp.login(username, password)
        
        if passive_mode:
            ftp.set_pasv(True)
        
        # Navigate to target directory
        remote_path = folder_path.strip('/')
        if remote_path:
            try:
                ftp.cwd(remote_path)
            except:
                # Directory doesn't exist, nothing to delete
                return 0
        
        # List all files in the directory
        files = ftp.nlst()
        
        for filename in files:
            try:
                # Check if file matches our database pattern
                if filename.startswith(f"{database}_") and filename.endswith(".sql.gz"):
                    # Extract date from filename
                    date_str = filename.replace(f"{database}_", "").replace(".sql.gz", "")
                    
                    # Parse the date from filename
                    file_date = datetime.strptime(date_str, '%Y-%m-%d')
                    
                    # Check if file is older than cutoff date
                    if file_date < cutoff_date:
                        ftp.delete(filename)
                        print(f"Deleted old FTP backup file: {filename}")
                        deleted_count += 1
                        
            except ValueError:
                # Skip files that don't match the expected date format
                continue
            except Exception as e:
                print(f"Error deleting FTP file {filename}: {e}")
                
    except Exception as e:
        print(f"Error in delete_old_ftp_files: {e}")
    finally:
        if ftp:
            try:
                ftp.quit()
            except:
                pass
    
    return deleted_count

def delete_old_blob_files(location, folder_path, database, cutoff_date):
    """
    Delete old backup files from Azure Blob Storage using SAS URL
    """
    deleted_count = 0
    
    try:
        config = json.loads(location.config)
        container_sas_url = config.get('connection_string', '')
        container_name = config.get('container', '')
        
        if not container_sas_url:
            return 0
        
        # Import Azure Blob Storage libraries
        try:
            from azure.storage.blob import ContainerClient
            from urllib.parse import urlparse
        except ImportError:
            print("Azure Blob Storage libraries not installed. Cannot delete old blob files.")
            return 0
        
        # Extract storage account name for logging
        try:
            parsed_url = urlparse(container_sas_url)
            storage_account_name = parsed_url.hostname.split('.')[0]
            print(f"DEBUG - Cleaning up old files from storage account: {storage_account_name}")
        except Exception as e:
            print(f"DEBUG - Could not extract storage account name: {e}")
        
        # Create ContainerClient using the container SAS URL
        container_client = ContainerClient.from_container_url(container_sas_url)
        
        # List blobs with the database prefix
        prefix = f"{folder_path}/{database}_" if folder_path else f"{database}_"
        
        try:
            blob_list = container_client.list_blobs(name_starts_with=prefix)
            
            for blob in blob_list:
                try:
                    # Extract date from blob name (format: folder/database_YYYY-MM-DD.sql.gz)
                    blob_name = blob.name
                    filename = os.path.basename(blob_name)
                    
                    # Check if filename matches our pattern
                    if filename.startswith(f"{database}_") and filename.endswith(".sql.gz"):
                        date_str = filename.replace(f"{database}_", "").replace(".sql.gz", "")
                        
                        # Parse the date from filename
                        file_date = datetime.strptime(date_str, '%Y-%m-%d')
                        
                        # Check if blob is older than cutoff date
                        if file_date < cutoff_date:
                            blob_client = container_client.get_blob_client(blob.name)
                            blob_client.delete_blob()
                            print(f"Deleted old blob: {blob.name}")
                            deleted_count += 1
                            
                except ValueError:
                    # Skip blobs that don't match the expected date format
                    continue
                except Exception as e:
                    print(f"Error deleting blob {blob.name}: {e}")
                    
        except Exception as e:
            print(f"Error listing blobs: {e}")
                
    except Exception as e:
        print(f"Error in delete_old_blob_files: {e}")
    
    return deleted_count