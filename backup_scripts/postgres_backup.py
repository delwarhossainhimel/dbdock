import os
import subprocess
import json
import gzip
import shutil
from datetime import datetime

from .storage_providers import get_storage_provider
from .utils import create_job_tmp_directory, cleanup_job_tmp_directory, create_full_folder_path
from job_schedules import get_enabled_schedule_types, get_schedule_retention, normalize_schedule_config


def get_pg_dump_binary(server):
    """
    Return the configured pg_dump binary.
    Falls back to plain 'pg_dump' if none is set.
    """
    from dump_clients import get_dump_clients_for_type

    dump_client = getattr(server, 'dump_client', None) or 'pg_dump'
    allowed = [c['value'] for c in get_dump_clients_for_type('postgres')]
    if dump_client not in allowed:
        raise RuntimeError(
            f"Configured pg_dump client '{dump_client}' is not in allowed list {allowed}"
        )
    if shutil.which(dump_client) is None:
        raise RuntimeError(
            f"Required pg_dump binary '{dump_client}' is not installed on the host."
        )
    print(f"🔧 Using configured pg_dump client: {dump_client}")
    return dump_client


def postgres_backup(server_id, databases, location_id, folder_path,
                    schedule_types, job_id=None, schedule_type=None):
    """
    PostgreSQL backup with process tracking for cancellation.
    Each database is: dumped -> compressed -> uploaded -> cleaned up.

    NOTE: Takes IDs (not ORM objects) so the caller can release its DB
    session before this long-running function starts. We open our own
    short-lived session here to re-fetch the server and location, snapshot
    every attribute we need into memory, then release the connection so
    the connection pool cannot time out an idle connection during the dump.
    """
    print(f"🔍 DEBUG: postgres_backup called with schedule_type={schedule_type}, job_id={job_id}")

    from scheduler import (
        register_backup_process,
        unregister_backup_process,
        register_child_process,
        unregister_child_process,
        is_job_cancelled,
    )

    # ─────────────────────────────────────────────────────────────────
    # Re-fetch server + location, snapshot attributes, release connection.
    # ─────────────────────────────────────────────────────────────────
    from app import app
    from models import DatabaseServer, StorageLocation, db

    with app.app_context():
        db.session.remove()   # ✅ start clean

        server = db.session.get(DatabaseServer, server_id)
        location = db.session.get(StorageLocation, location_id)

        if server is None:
            raise ValueError(f"DatabaseServer id={server_id} not found")
        if location is None:
            raise ValueError(f"StorageLocation id={location_id} not found")

        db.session.refresh(server)
        db.session.refresh(location)

        # ✅ Log exactly which dump_client we're about to use
        print(f"🔎 [postgres_backup] server_id={server_id} "
            f"dump_client={getattr(server, 'dump_client', None)!r}")

        _ = (
            server.id, server.name, server.host, server.port,
            server.username, server.password,
            server.type,
            server.type.value if hasattr(server.type, "value") else server.type,
            getattr(server, "dump_client", None),

            location.id, location.name,
            location.type,
            location.type.value if hasattr(location.type, "value") else location.type,
            location.config,

            getattr(location, "path", None),
            getattr(location, "host", None),
            getattr(location, "username", None),
            getattr(location, "password", None),
            getattr(location, "bucket_name", None),
        )

        db.session.remove()

    job_tmp_dir = create_job_tmp_directory(job_id, schedule_type)

    try:
        print(f"Backup targets: {schedule_types}")
        print(f"Job temporary directory: {job_tmp_dir}")

        is_all_databases = False
        if 'all' in databases or '__all__' in databases:
            is_all_databases = True
            print("🔄 Auto-detecting all databases from PostgreSQL server...")
            databases = get_all_postgres_databases(server)
            if not databases:
                return False, "No databases found on the server", None, 0, []
            print(f"✅ Auto-detected {len(databases)} databases: {', '.join(databases)}")
        else:
            print(f"📊 Using manually selected databases: {', '.join(databases)}")

        print("🔍 Verifying databases exist...")
        valid_databases = []
        invalid_databases = []

        for database in databases:
            if check_database_exists(server, database):
                valid_databases.append(database)
            else:
                print(f"✗ Database '{database}' does NOT exist on the server!")
                invalid_databases.append(database)

        if not valid_databases:
            cleanup_job_tmp_directory(job_tmp_dir)
            return False, "No valid databases found to backup", None, 0, []

        print(f"✅ Found {len(valid_databases)} valid databases: {', '.join(valid_databases)}")

        database_results = []
        all_backup_files = []
        total_success = 0
        total_failed = 0
        total_cancelled = 0
        uploaded_files = []
        total_size = 0
        was_cancelled = False

        # ✅ Resolve the dump binary ONCE (fails fast if the binary is missing)
        pg_dump_bin = get_pg_dump_binary(server)

        for database in valid_databases:
            if is_job_cancelled(job_id, schedule_type):
                was_cancelled = True
                print(f"🛑 Backup cancelled by user, stopping after {database}")
                remaining_databases = valid_databases[valid_databases.index(database):]
                for remaining_db in remaining_databases:
                    database_results.append({
                        'database': remaining_db,
                        'status': 'cancelled',
                        'message': 'Skipped - backup cancelled by user',
                        'file_size': 0,
                    })
                    total_cancelled += 1
                break

            print(f"\n{'='*60}")
            print(f"📦 Processing database: {database}")
            print(f"{'='*60}")

            timestamp = datetime.now().strftime('%Y-%m-%d')

            sql_filename = f"{database}_{timestamp}.sql"
            sql_filepath = os.path.join(job_tmp_dir, sql_filename)

            print(f"📝 Step 1: Dumping database '{database}' to {sql_filename}...")

            env = os.environ.copy()
            env['PGPASSWORD'] = server.password

            # ✅ Use the resolved variable (NOT the quoted string 'pg_dump_bin')
            pg_dump_cmd = [
                pg_dump_bin,
                '-h', server.host,
                '-p', str(server.port),
                '-U', server.username,
                '-F', 'p',   # Plain format
                '-C',        # Include CREATE DATABASE statement
                database,
            ]

            dump_success = False
            dump_error = ""
            process = None

            try:
                process = subprocess.Popen(
                    pg_dump_cmd,
                    env=env,
                    stdout=open(sql_filepath, 'w'),
                    stderr=subprocess.PIPE,
                    text=True,
                    preexec_fn=os.setsid,
                )

                register_backup_process(job_id, schedule_type, process, None)

                if is_all_databases:
                    register_child_process(job_id, schedule_type, process)

                stdout, stderr = process.communicate()

                if is_all_databases and process:
                    unregister_child_process(job_id, schedule_type, process)

                if process.returncode == 0:
                    dump_success = True
                    sql_size = os.path.getsize(sql_filepath) if os.path.exists(sql_filepath) else 0
                    print(f"   ✅ Dump completed successfully! Size: {sql_size} bytes")
                else:
                    dump_success = False
                    dump_error = stderr.strip() if stderr else "Unknown error"
                    print(f"   ❌ Dump failed! Error: {dump_error}")

            except Exception as e:
                dump_success = False
                dump_error = str(e)
                print(f"   ❌ Dump failed with exception: {dump_error}")
            finally:
                unregister_backup_process(job_id, schedule_type)
                if is_all_databases and process:
                    unregister_child_process(job_id, schedule_type, process)

            if not dump_success:
                database_results.append({
                    'database': database,
                    'status': 'failed',
                    'message': f"Dump failed: {dump_error}",
                    'file_size': 0,
                })
                total_failed += 1

                if os.path.exists(sql_filepath):
                    os.remove(sql_filepath)
                    print(f"   🗑️ Removed partial dump file")
                if is_job_cancelled(job_id, schedule_type):
                    was_cancelled = True
                    print(f"🛑 Backup cancelled by user, stopping after {database}")
                    break
                continue

            # Compress
            gz_filename = f"{database}_{timestamp}.sql.gz"
            gz_filepath = os.path.join(job_tmp_dir, gz_filename)

            print(f"📦 Step 2: Compressing {sql_filename} to {gz_filename}...")

            compress_success = False
            compress_error = ""
            compressed_size = 0

            try:
                with open(sql_filepath, 'rb') as f_in:
                    with gzip.open(gz_filepath, 'wb', compresslevel=6) as f_out:
                        shutil.copyfileobj(f_in, f_out)

                compressed_size = os.path.getsize(gz_filepath) if os.path.exists(gz_filepath) else 0
                compress_success = True
                print(f"   ✅ Compression completed! Compressed size: {compressed_size} bytes")

            except Exception as e:
                compress_success = False
                compress_error = str(e)
                print(f"   ❌ Compression failed: {compress_error}")

            if not compress_success:
                database_results.append({
                    'database': database,
                    'status': 'failed',
                    'message': f"Compression failed: {compress_error}",
                    'file_size': 0,
                })
                total_failed += 1

                if os.path.exists(sql_filepath):
                    os.remove(sql_filepath)
                if os.path.exists(gz_filepath):
                    os.remove(gz_filepath)
                if is_job_cancelled(job_id, schedule_type):
                    was_cancelled = True
                    print(f"🛑 Backup cancelled by user, stopping after {database}")
                    break
                continue

            # Upload
            print(f"📤 Step 3: Uploading {gz_filename} to storage...")

            upload_success = False
            upload_message = ""
            uploaded_path = ""

            try:
                for schedule_type_item in schedule_types:
                    full_folder_path = create_full_folder_path(folder_path, schedule_type_item)
                    print(f"   📤 Uploading to: {full_folder_path}")

                    config = json.loads(location.config)
                    storage_type = location.type.value
                    storage_provider = get_storage_provider(storage_type)

                    if storage_provider:
                        upload_result = storage_provider.upload_files(
                            config,
                            full_folder_path,
                            [(gz_filepath, gz_filename)]
                        )

                        if upload_result[0]:
                            upload_success = True
                            uploaded_path = upload_result[2] if upload_result[2] else gz_filename
                            print(f"   ✅ Uploaded successfully: {uploaded_path}")

                            all_backup_files.append((gz_filepath, gz_filename))
                            total_size += compressed_size
                            uploaded_files.append(uploaded_path)
                        else:
                            print(f"   ❌ Upload failed: {upload_result[1]}")
                            upload_message = upload_result[1]
                    else:
                        print(f"   ❌ Storage provider not found for type: {storage_type}")
                        upload_message = f"Storage provider not found for type: {storage_type}"

            except Exception as e:
                print(f"   ❌ Upload error: {str(e)}")
                upload_success = False
                upload_message = str(e)

            if not upload_success:
                database_results.append({
                    'database': database,
                    'status': 'failed',
                    'message': f"Upload failed: {upload_message}",
                    'file_size': compressed_size if compress_success else 0,
                })
                total_failed += 1
            else:
                database_results.append({
                    'database': database,
                    'status': 'success',
                    'message': 'Backup completed successfully',
                    'file_size': compressed_size,
                })
                total_success += 1

            # Cleanup temp files
            print(f"🧹 Step 4: Cleaning up temp files for {database}...")
            try:
                if os.path.exists(sql_filepath):
                    os.remove(sql_filepath)
                    print(f"   🗑️ Removed: {sql_filename}")
                if os.path.exists(gz_filepath):
                    os.remove(gz_filepath)
                    print(f"   🗑️ Removed: {gz_filename}")
            except Exception as e:
                print(f"   ⚠️ Error cleaning up: {e}")

            print(f"✅ Finished processing database: {database}")

            if is_job_cancelled(job_id, schedule_type):
                was_cancelled = True
                print(f"🛑 Backup cancelled by user, stopping after {database}")
                remaining_databases = valid_databases[valid_databases.index(database) + 1:]
                for remaining_db in remaining_databases:
                    database_results.append({
                        'database': remaining_db,
                        'status': 'cancelled',
                        'message': 'Skipped - backup cancelled by user',
                        'file_size': 0,
                    })
                    total_cancelled += 1
                break

        # Clean tmp dir
        print(f"\n🧹 Cleaning up temporary directory: {job_tmp_dir}")
        try:
            if os.path.exists(job_tmp_dir):
                os.rmdir(job_tmp_dir)
                print(f"   ✅ Removed directory: {job_tmp_dir}")
        except Exception as e:
            print(f"   ⚠️ Could not remove directory: {e}")

        # Retention
        print(f"\n🗄️ Applying retention policy...")
        for schedule_type_item in schedule_types:
            apply_retention_policy(location, folder_path, schedule_type_item)

        uploaded_files_str = ";".join(uploaded_files) if uploaded_files else None

        if was_cancelled:
            skipped = len(valid_databases) - total_success - total_failed - total_cancelled
            if total_success > 0 or total_failed > 0:
                message = (f"Backup cancelled by user after {total_success} successful, "
                           f"{total_failed} failed, {skipped} skipped")
            else:
                message = "Backup cancelled by user before completion"
            return False, message, uploaded_files_str, total_size, database_results

        if total_failed == 0 and total_success > 0:
            message = f"Successfully backed up all {total_success} databases"
            return True, message, uploaded_files_str, total_size, database_results
        elif total_success > 0 and total_failed > 0:
            message = (f"Partial success: {total_success} succeeded, "
                       f"{total_failed} failed out of {len(databases)} databases")
            return False, message, uploaded_files_str, total_size, database_results
        elif total_success == 0 and total_failed > 0:
            message = f"All {total_failed} databases failed to backup"
            return False, message, None, 0, database_results
        else:
            return False, "No databases were processed", None, 0, database_results

    except Exception as e:
        print(f"💥 Critical error in postgres_backup: {e}")
        import traceback
        traceback.print_exc()
        cleanup_job_tmp_directory(job_tmp_dir)
        return False, str(e), None, 0, []


def check_database_exists(server, database):
    """Check if a database exists on the PostgreSQL server"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=server.host,
            port=server.port,
            user=server.username,
            password=server.password,
            database='postgres',
            connect_timeout=10
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result is not None
    except Exception as e:
        print(f"⚠️ Error checking database '{database}': {e}")
        return False


def get_all_postgres_databases(server):
    """Connect to PostgreSQL server and get all databases excluding system databases"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=server.host,
            port=server.port,
            user=server.username,
            password=server.password,
            database='postgres',
            connect_timeout=10
        )
        cursor = conn.cursor()
        cursor.execute(
            "SELECT datname FROM pg_database "
            "WHERE datistemplate = false AND datname != 'postgres'"
        )
        databases = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return databases
    except Exception as e:
        print(f"⚠️ Error detecting databases from PostgreSQL server: {e}")
        return []


def upload_to_storage(location, folder_path, backup_files):
    """Upload backup files using the appropriate storage provider"""
    config = json.loads(location.config)
    storage_type = location.type.value

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

    return True, "Backup completed successfully", ";".join(uploaded_paths), total_size


def build_partial_failure_message(database_results):
    succeeded = len([item for item in database_results if item['status'] == 'success'])
    failed = len(database_results) - succeeded
    return f"Backup completed with partial failures. {succeeded} succeeded, {failed} failed."


def apply_retention_policy(location, base_folder_path, schedule_type):
    """Apply retention policy by deleting old backup files"""
    try:
        from app import app
        from models import BackupJob
        from datetime import datetime, timedelta

        full_folder_path = create_full_folder_path(base_folder_path, schedule_type)

        with app.app_context():
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

                databases = json.loads(job.databases)

                if 'all' in databases or '__all__' in databases:
                    try:
                        server = job.database_server
                        if server.type.value == 'postgres':
                            actual_databases = get_all_postgres_databases(server)
                            if actual_databases:
                                databases = actual_databases
                    except Exception as e:
                        print(f"⚠️ Could not auto-detect databases for retention: {e}")
                        continue

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

    storage_provider = get_storage_provider(storage_type)
    if not storage_provider:
        print(f"Unsupported storage type for deletion: {storage_type}")
        return 0

    try:
        return storage_provider.delete_old_files(config, folder_path, database, cutoff_date)
    except Exception as e:
        print(f"Error deleting old backup files: {e}")
        return 0