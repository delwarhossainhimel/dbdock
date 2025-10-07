import os
import json
import shutil
import ftplib
import socket
from urllib.parse import urlparse
from datetime import datetime

class StorageProvider:
    """Base class for all storage providers"""
    
    def upload_files(self, config, folder_path, backup_files):
        raise NotImplementedError("Subclasses must implement upload_files")
    
    def delete_old_files(self, config, folder_path, database, cutoff_date):
        raise NotImplementedError("Subclasses must implement delete_old_files")

class LocalStorageProvider(StorageProvider):
    """Local file system storage"""
    
    def upload_files(self, config, folder_path, backup_files):
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
    
    def delete_old_files(self, config, folder_path, database, cutoff_date):
        deleted_count = 0
        try:
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

class FTPStorageProvider(StorageProvider):
    """FTP storage provider"""
    
    def upload_files(self, config, folder_path, backup_files):
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
            self._create_ftp_directory(ftp, remote_path)
            
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
    
    def delete_old_files(self, config, folder_path, database, cutoff_date):
        deleted_count = 0
        ftp = None
        
        try:
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
    
    def _create_ftp_directory(self, ftp, remote_path):
        """Create directory structure on FTP server"""
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
                except ftplib.error_perm:
                    # Try to create the directory
                    try:
                        ftp.mkd(part)
                        print(f"✓ Created directory: {part}")
                        # Try to enter the newly created directory
                        try:
                            ftp.cwd(part)
                        except:
                            pass
                    except ftplib.error_perm as mkdir_error:
                        mkdir_error_msg = str(mkdir_error)
                        if "550" in mkdir_error_msg and "exists" in mkdir_error_msg.lower():
                            print(f"✓ Directory already exists (despite error): {part}")
            
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

class AzureBlobStorageProvider(StorageProvider):
    """Azure Blob Storage provider"""
    
    def upload_files(self, config, folder_path, backup_files):
        try:
            # Extract Azure Blob Storage configuration
            container_sas_url = config.get('connection_string', '')
            container_name = config.get('container', '')
            
            # Debug output
            print(f"DEBUG - Container SAS URL: {container_sas_url.split('?')[0]}")
            print(f"DEBUG - Container Name: '{container_name}'")
            
            if not container_sas_url:
                return False, "Azure Blob Storage SAS URL not configured", None, 0
            
            print(f"Connecting to Azure Blob Storage using container SAS URL")
            
            # Import Azure Blob Storage libraries
            try:
                from azure.storage.blob import ContainerClient
            except ImportError:
                return False, "Azure Blob Storage libraries not installed. Please install azure-storage-blob", None, 0
            
            # Extract storage account name from SAS URL dynamically
            try:
                parsed_url = urlparse(container_sas_url)
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
    
    def delete_old_files(self, config, folder_path, database, cutoff_date):
        deleted_count = 0
        
        try:
            container_sas_url = config.get('connection_string', '')
            container_name = config.get('container', '')
            
            if not container_sas_url:
                return 0
            
            # Import Azure Blob Storage libraries
            try:
                from azure.storage.blob import ContainerClient
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

class S3StorageProvider(StorageProvider):
    """S3 Storage provider (not implemented)"""
    
    def upload_files(self, config, folder_path, backup_files):
        return False, "S3 storage not yet implemented", None, 0
    
    def delete_old_files(self, config, folder_path, database, cutoff_date):
        return 0

# Storage provider registry
STORAGE_PROVIDERS = {
    'local': LocalStorageProvider(),
    'ftp': FTPStorageProvider(),
    'blob': AzureBlobStorageProvider(),
    's3': S3StorageProvider()
}

def get_storage_provider(storage_type):
    """Get storage provider by type"""
    return STORAGE_PROVIDERS.get(storage_type)