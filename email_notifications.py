"""
Email notification helpers for dbDock.

All SMTP configuration, sending, and HTML/text template logic lives here.
`scheduler.py` imports the public functions it needs:
    - send_notification_email
    - send_cancellation_email
    - send_test_email

Each public function takes *IDs* (job_id, history_id) rather than ORM
objects, and re-fetches what it needs on its own short-lived session.
This avoids DetachedInstanceError when the scheduler has already
released its DB session by the time the email is sent.
"""

import html
import os
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr

from models import BackupHistory, BackupJob


# ─────────────────────────────────────────────────────────────────
# SMTP configuration
# ─────────────────────────────────────────────────────────────────

def get_smtp_config(job):
    """
    Build SMTP config. Accepts a *freshly-loaded* BackupJob instance
    (never a detached one).
    """
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


def login_and_send(server, smtp_config, email_message):
    if smtp_config['username']:
        server.login(smtp_config['username'], smtp_config['password'])
    server.sendmail(
        smtp_config['from_email'],
        [smtp_config['receiver_email']],
        email_message.as_string(),
    )


def _send_email(smtp_config, email_message):
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


# ─────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────

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


def get_email_schedule_label(schedule_targets):
    if not schedule_targets:
        return "Backup"
    if len(schedule_targets) == 1:
        return schedule_targets[0].capitalize()
    return "Combined"


def summarize_database_results(database_results):
    success_count = len([item for item in database_results if item.get('status') == 'success'])
    total_count = len(database_results)
    failed_count = total_count - success_count
    return {
        'success_count': success_count,
        'failed_count': failed_count,
        'total_count': total_count,
    }


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


# ─────────────────────────────────────────────────────────────────
# Success / failure report templates
# ─────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────
# Public API — success/failure email
# ─────────────────────────────────────────────────────────────────

def send_notification_email(job_id, history_id, schedule_targets, success, message, database_results):
    """
    Send an HTML backup report email via SMTP using environment variables.

    Takes IDs (not ORM objects) and re-fetches the job + history on a
    fresh short-lived session so it works even after the scheduler has
    released its DB session.
    """
    from app import app
    from models import db

    try:
        with app.app_context():
            job = db.session.get(BackupJob, job_id)
            history = db.session.get(BackupHistory, history_id)

            if job is None:
                print(f"⚠️ Cannot send notification: BackupJob id={job_id} not found")
                return
            if history is None:
                print(f"⚠️ Cannot send notification: BackupHistory id={history_id} not found")
                return

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

            _send_email(smtp_config, email_message)
            print(f"📧 Notification email sent to: {smtp_config['receiver_email']}")
    except Exception as e:
        print(f"⚠️ Error sending notification email: {e}")


# ─────────────────────────────────────────────────────────────────
# Cancellation templates
# ─────────────────────────────────────────────────────────────────

def build_cancellation_email_html(job, history, schedule_label, database_results):
    summary = summarize_database_results(database_results)
    status_rows = ''.join(
        [
            (
                "<tr>"
                f"<td style=\"padding:10px;border:1px solid #d9d9d9;\">{html.escape(item.get('database', 'Unknown'))}</td>"
                f"<td style=\"padding:10px;border:1px solid #d9d9d9;color:{'#0a8f08' if item.get('status') == 'success' else '#c62828' if item.get('status') == 'failed' else '#856404'};font-weight:700;\">"
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

    return f"""
    <html>
      <body style="font-family: Arial, sans-serif; background:#ffffff; color:#222; padding:20px;">
        <div style="max-width:760px; margin:0 auto;">
          <h2 style="text-align:center; margin-bottom:24px; color:#856404;">⛔ {html.escape(schedule_label)} Backup CANCELLED</h2>

          <div style="background:#fff3cd; border:1px solid #ffc107; border-radius:8px; padding:15px; margin-bottom:20px;">
            <p style="margin:0; font-weight:700; color:#856404;">
              ⚠️ The backup was cancelled by the user at {history.cancelled_at.strftime('%Y-%m-%d %H:%M:%S') if history.cancelled_at else 'Unknown time'}.
            </p>
          </div>

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
                  <strong>Summary:</strong> {summary['success_count']} succeeded, {summary['failed_count']} failed out of {summary['total_count']} databases
                </td>
              </tr>
            </tbody>
          </table>

          <table style="width:100%; border-collapse:collapse; margin-bottom:16px;">
            <thead>
              <tr style="background:#f2f2f2;">
                <th style="padding:10px; border:1px solid #d9d9d9;">Job Details</th>
                <th style="padding:10px; border:1px solid #d9d9d9;">Value</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td style="padding:10px; border:1px solid #d9d9d9;"><strong>Job Name</strong></td>
                <td style="padding:10px; border:1px solid #d9d9d9;">{html.escape(job.name)}</td>
              </tr>
              <tr>
                <td style="padding:10px; border:1px solid #d9d9d9;"><strong>Started At</strong></td>
                <td style="padding:10px; border:1px solid #d9d9d9;">{history.start_time.strftime('%Y-%m-%d %H:%M:%S')}</td>
              </tr>
              <tr>
                <td style="padding:10px; border:1px solid #d9d9d9;"><strong>Schedule Type</strong></td>
                <td style="padding:10px; border:1px solid #d9d9d9;">{html.escape(schedule_label)}</td>
              </tr>
              <tr>
                <td style="padding:10px; border:1px solid #d9d9d9;"><strong>Storage Location</strong></td>
                <td style="padding:10px; border:1px solid #d9d9d9;">{html.escape(job.storage_location.name)}</td>
              </tr>
            </tbody>
          </table>

          <p style="text-align:center; margin-top:20px; color:#856404;">
            <strong>⚠️ Backup was cancelled. No further action was taken.</strong>
          </p>
          <p style="text-align:center; margin-top:10px; font-size:12px; color:#6c757d;">
            Log file available at: {html.escape(history.log_path or 'No log file available')}
          </p>
        </div>
      </body>
    </html>
    """


def build_cancellation_email_text(job, history, schedule_label, database_results):
    summary = summarize_database_results(database_results)
    lines = [
        f"⛔ {schedule_label} Backup CANCELLED",
        "=" * 50,
        "",
        f"⚠️ The backup was cancelled by the user at {history.cancelled_at.strftime('%Y-%m-%d %H:%M:%S') if history.cancelled_at else 'Unknown time'}.",
        "",
        "Database Status:",
    ]

    for item in database_results:
        lines.append(f"- {item.get('database', 'Unknown')}: {item.get('status', 'unknown').capitalize()}")

    lines.extend(
        [
            "",
            f"Summary: {summary['success_count']} succeeded, {summary['failed_count']} failed out of {summary['total_count']} databases",
            "",
            "Job Details:",
            f"  Job Name: {job.name}",
            f"  Started At: {history.start_time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"  Schedule Type: {schedule_label}",
            f"  Storage Location: {job.storage_location.name}",
            "",
            "⚠️ Backup was cancelled. No further action was taken.",
            f"Log file available at: {history.log_path or 'No log file available'}",
        ]
    )

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────
# Public API — cancellation email
# ─────────────────────────────────────────────────────────────────

def send_cancellation_email(job_id, history_id, schedule_targets, database_results, success):
    """
    Send a cancellation email notification.

    Takes IDs (not ORM objects) and re-fetches the job + history on a
    fresh short-lived session.
    """
    from app import app
    from models import db

    try:
        with app.app_context():
            job = db.session.get(BackupJob, job_id)
            history = db.session.get(BackupHistory, history_id)

            if job is None:
                print(f"⚠️ Cannot send cancellation email: BackupJob id={job_id} not found")
                return
            if history is None:
                print(f"⚠️ Cannot send cancellation email: BackupHistory id={history_id} not found")
                return

            smtp_config = get_smtp_config(job)
            if not smtp_config['host'] or not smtp_config['receiver_email'] or not smtp_config['from_email']:
                print("⚠️ SMTP configuration is incomplete. Skipping cancellation email.")
                return

            schedule_label = get_email_schedule_label(schedule_targets)
            subject = f"{schedule_label} backup CANCELLED - {job.name}"

            html_body = build_cancellation_email_html(job, history, schedule_label, database_results)
            text_body = build_cancellation_email_text(job, history, schedule_label, database_results)

            email_message = MIMEMultipart('alternative')
            email_message['Subject'] = subject
            email_message['From'] = formataddr((smtp_config['from_name'], smtp_config['from_email']))
            email_message['To'] = smtp_config['receiver_email']

            email_message.attach(MIMEText(text_body, 'plain'))
            email_message.attach(MIMEText(html_body, 'html'))

            _send_email(smtp_config, email_message)
            print(f"📧 Cancellation email sent to: {smtp_config['receiver_email']}")
    except Exception as e:
        print(f"⚠️ Error sending cancellation email: {e}")


# ─────────────────────────────────────────────────────────────────
# Public API — SMTP test email
# ─────────────────────────────────────────────────────────────────

def send_test_email(recipient_email):
    """Send a simple SMTP test email to verify configuration."""
    from app import app

    with app.app_context():
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

        _send_email(smtp_config, email_message)