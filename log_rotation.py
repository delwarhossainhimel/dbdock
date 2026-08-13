"""
Log rotation utility for backup jobs
"""
import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta

# Configuration - can be overridden by environment variables
MAX_LOG_SIZE = int(os.getenv('MAX_LOG_SIZE', 10 * 1024 * 1024))  # 10 MB default
RETENTION_DAYS = int(os.getenv('LOG_RETENTION_DAYS', 15))  # 15 days default
MAX_ROTATED_LOGS = int(os.getenv('MAX_ROTATED_LOGS', 10))  # Keep up to 10 rotated logs


def rotate_log(log_path):
    """
    Rotate log file if it exceeds max size.
    Creates backup files: job.log.1, job.log.2, etc.
    """
    log_path = Path(log_path)
    
    if not log_path.exists():
        return False
    
    # Rotate only if file exceeds the size limit
    if log_path.stat().st_size < MAX_LOG_SIZE:
        return False
    
    try:
        # Shift older backups:
        # job.log.9 -> job.log.10, job.log.8 -> job.log.9, etc.
        for i in range(MAX_ROTATED_LOGS - 1, 0, -1):
            old = log_path.with_name(f"{log_path.name}.{i}")
            new = log_path.with_name(f"{log_path.name}.{i + 1}")
            
            if old.exists():
                old.rename(new)
        
        # job.log -> job.log.1
        if log_path.exists():
            shutil.move(log_path, log_path.with_name(f"{log_path.name}.1"))
        
        # Create a new empty log
        log_path.touch()
        print(f"🔄 Log rotated: {log_path}")
        return True
        
    except Exception as e:
        print(f"⚠️ Error rotating log {log_path}: {e}")
        return False


def cleanup_old_rotated_logs(log_dir):
    """
    Delete rotated logs older than RETENTION_DAYS
    """
    log_dir = Path(log_dir)
    if not log_dir.exists():
        return 0
    
    cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)
    deleted_count = 0
    
    try:
        for file in log_dir.glob("*.log.*"):
            try:
                modified = datetime.fromtimestamp(file.stat().st_mtime)
                if modified < cutoff:
                    file.unlink()
                    deleted_count += 1
                    print(f"🧹 Deleted old rotated log: {file}")
            except (OSError, ValueError) as e:
                print(f"⚠️ Error processing {file}: {e}")
    except Exception as e:
        print(f"⚠️ Error cleaning up old logs: {e}")
    
    return deleted_count


def get_log_file_size(log_path):
    """
    Get the size of a log file in bytes
    """
    log_path = Path(log_path)
    if not log_path.exists():
        return 0
    return log_path.stat().st_size


def get_log_rotation_info(log_path):
    """
    Get information about rotated log files
    """
    log_path = Path(log_path)
    log_dir = log_path.parent
    base_name = log_path.name
    
    rotated_files = []
    for i in range(1, MAX_ROTATED_LOGS + 1):
        rotated_path = log_dir / f"{base_name}.{i}"
        if rotated_path.exists():
            rotated_files.append({
                'name': rotated_path.name,
                'size': rotated_path.stat().st_size,
                'modified': datetime.fromtimestamp(rotated_path.stat().st_mtime)
            })
    
    return {
        'current_log': {
            'name': log_path.name,
            'size': get_log_file_size(log_path),
            'exists': log_path.exists()
        },
        'rotated_logs': rotated_files,
        'max_size': MAX_LOG_SIZE,
        'retention_days': RETENTION_DAYS,
        'max_rotated_logs': MAX_ROTATED_LOGS
    }


def cleanup_logs_by_size(log_dir, max_total_size=None):
    """
    Clean up logs if total size exceeds max_total_size
    """
    log_dir = Path(log_dir)
    if not log_dir.exists():
        return 0
    
    if max_total_size is None:
        max_total_size = MAX_LOG_SIZE * MAX_ROTATED_LOGS * 2  # Default: 2x the max size
    
    # Get all log files (current and rotated)
    log_files = list(log_dir.glob("*.log*"))
    
    if not log_files:
        return 0
    
    # Sort by modification time (oldest first)
    log_files.sort(key=lambda f: f.stat().st_mtime)
    
    # Calculate total size
    total_size = sum(f.stat().st_size for f in log_files)
    
    if total_size <= max_total_size:
        return 0
    
    # Delete oldest logs until total size is under limit
    deleted_count = 0
    for log_file in log_files:
        if total_size <= max_total_size:
            break
        try:
            file_size = log_file.stat().st_size
            log_file.unlink()
            total_size -= file_size
            deleted_count += 1
            print(f"🧹 Deleted log file to free space: {log_file}")
        except Exception as e:
            print(f"⚠️ Error deleting {log_file}: {e}")
    
    return deleted_count


# Example usage
if __name__ == "__main__":
    # Test the rotation
    import tempfile
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_log = Path(tmpdir) / "test.log"
        
        # Create a large log file
        with open(test_log, 'w') as f:
            f.write("x" * (MAX_LOG_SIZE + 1000))
        
        print(f"Created test log: {test_log} ({test_log.stat().st_size} bytes)")
        
        # Rotate
        rotate_log(test_log)
        
        # Check rotation
        info = get_log_rotation_info(test_log)
        print(f"Rotation info: {info}")
        
        # Cleanup
        cleanup_old_rotated_logs(Path(tmpdir))