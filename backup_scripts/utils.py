import os
import shutil
import uuid
from datetime import datetime

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

def create_full_folder_path(base_folder_path, schedule_type):
    """
    Create the full folder path by appending schedule type as subfolder
    """
    # Capitalize the first letter of schedule type
    schedule_folder = schedule_type.capitalize()
    
    # Combine base path with schedule folder
    if base_folder_path:
        full_path = f"{base_folder_path}/{schedule_folder}"
    else:
        full_path = schedule_folder
    
    return full_path

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