import json

SCHEDULE_TYPES = ("daily", "weekly", "monthly", "yearly")


def default_schedule_config():
    return {
        "daily": {"enabled": False, "time": "00:00", "retention": "1"},
        "weekly": {"enabled": False, "day": "0", "time": "00:00", "retention": "1"},
        "monthly": {"enabled": False, "day": "1", "time": "00:00", "retention": "1"},
        "yearly": {"enabled": False, "month": "1", "day": "1", "time": "00:00", "retention": "1"},
    }


def normalize_schedule_config(schedule_config=None, schedule_type=None, cron_expression=None, fallback_retention=None):
    config = default_schedule_config()

    if schedule_config:
        if isinstance(schedule_config, str):
            schedule_config = json.loads(schedule_config)

        for schedule_name in SCHEDULE_TYPES:
            if schedule_name not in schedule_config:
                continue

            raw_value = schedule_config[schedule_name] or {}
            if isinstance(raw_value, dict):
                config[schedule_name].update(raw_value)
                config[schedule_name]["enabled"] = bool(raw_value.get("enabled", False))

    if schedule_type and cron_expression:
        apply_legacy_schedule(config, schedule_type, cron_expression)

    apply_fallback_retention(config, fallback_retention)

    return config


def apply_legacy_schedule(config, schedule_type, cron_expression):
    cron_parts = (cron_expression or "").split()
    if schedule_type not in SCHEDULE_TYPES or len(cron_parts) != 5:
        return

    minute, hour, day, month, day_of_week = cron_parts
    config[schedule_type]["enabled"] = True
    config[schedule_type]["time"] = f"{hour.zfill(2)}:{minute.zfill(2)}"

    if schedule_type == "weekly":
        config["weekly"]["day"] = day_of_week
    elif schedule_type == "monthly":
        config["monthly"]["day"] = day
    elif schedule_type == "yearly":
        config["yearly"]["day"] = day
        config["yearly"]["month"] = month

def build_schedule_config_from_form(form_data):
    config = default_schedule_config()
    selected = set(form_data.getlist("schedule_types"))
    
    # Get the master time
    master_time = form_data.get("master_time", "00:00")
    
    config["daily"]["enabled"] = "daily" in selected
    config["daily"]["time"] = master_time
    config["daily"]["retention"] = form_data.get("daily_retention", "7") or "7"
    
    config["weekly"]["enabled"] = "weekly" in selected
    config["weekly"]["day"] = form_data.get("weekly_day", "0") or "0"
    config["weekly"]["time"] = master_time
    config["weekly"]["retention"] = form_data.get("weekly_retention", "4") or "4"
    
    config["monthly"]["enabled"] = "monthly" in selected
    config["monthly"]["day"] = form_data.get("monthly_day", "1") or "1"
    config["monthly"]["time"] = master_time
    config["monthly"]["retention"] = form_data.get("monthly_retention", "12") or "12"
    
    config["yearly"]["enabled"] = "yearly" in selected
    config["yearly"]["month"] = form_data.get("yearly_month", "1") or "1"
    config["yearly"]["day"] = form_data.get("yearly_day", "1") or "1"
    config["yearly"]["time"] = master_time
    config["yearly"]["retention"] = form_data.get("yearly_retention", "5") or "5"
    
    return config

def validate_schedule_config(config):
    enabled_types = get_enabled_schedule_types(config)
    if not enabled_types:
        return False, "Select at least one schedule."

    for schedule_type in enabled_types:
        details = config[schedule_type]

        if not is_valid_time(details.get("time")):
            return False, f"Invalid time for {schedule_type} schedule."
        if not is_valid_retention(details.get("retention")):
            return False, f"Retention for {schedule_type} must be 1 or greater."

        if schedule_type == "weekly" and details.get("day") not in {"0", "1", "2", "3", "4", "5", "6"}:
            return False, "Weekly day must be between Sunday and Saturday."

        if schedule_type == "monthly":
            try:
                day = int(details.get("day", 1))
            except (TypeError, ValueError):
                day = 0
            if day < 1 or day > 31:
                return False, "Monthly day must be between 1 and 31."

        if schedule_type == "yearly":
            try:
                month = int(details.get("month", 1))
                day = int(details.get("day", 1))
            except (TypeError, ValueError):
                month = 0
                day = 0
            if month < 1 or month > 12:
                return False, "Yearly month must be between 1 and 12."
            if day < 1 or day > 31:
                return False, "Yearly day must be between 1 and 31."

    return True, None


def is_valid_time(value):
    if not value or ":" not in value:
        return False

    parts = value.split(":", 1)
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return False

    return 0 <= hour <= 23 and 0 <= minute <= 59


def is_valid_retention(value):
    try:
        return int(value) >= 1
    except (TypeError, ValueError):
        return False


def get_enabled_schedule_types(config):
    normalized = normalize_schedule_config(config)
    return [schedule_type for schedule_type in SCHEDULE_TYPES if normalized[schedule_type].get("enabled")]


def get_primary_schedule_type(config):
    enabled_types = get_enabled_schedule_types(config)
    return enabled_types[0] if enabled_types else None


def get_cron_expression(schedule_type, details):
    time_value = details.get("time", "00:00")
    hour, minute = parse_time(time_value)

    if schedule_type == "daily":
        return f"{minute} {hour} * * *"
    if schedule_type == "weekly":
        return f"{minute} {hour} * * {details.get('day', '0')}"
    if schedule_type == "monthly":
        return f"{minute} {hour} {details.get('day', '1')} * *"
    if schedule_type == "yearly":
        return f"{minute} {hour} {details.get('day', '1')} {details.get('month', '1')} *"
    return None


def get_schedule_entries(config):
    normalized = normalize_schedule_config(config)
    entries = []

    for schedule_type in get_enabled_schedule_types(normalized):
        details = normalized[schedule_type]
        entries.append(
            {
                "type": schedule_type,
                "label": schedule_type.capitalize(),
                "details": details,
                "cron_expression": get_cron_expression(schedule_type, details),
                "summary": format_schedule_entry(schedule_type, details),
            }
        )

    return entries

def format_schedule_entry(schedule_type, details):
    time_value = details.get("time", "00:00")
    retention = details.get("retention", "1")

    if schedule_type == "daily":
        return f"Daily at {time_value} | keep {retention} day(s)"
    if schedule_type == "weekly":
        weekday_names = {
            "0": "Sunday",
            "1": "Monday",
            "2": "Tuesday",
            "3": "Wednesday",
            "4": "Thursday",
            "5": "Friday",
            "6": "Saturday",
        }
        return f"Weekly on {weekday_names.get(details.get('day', '0'), 'Sunday')} at {time_value} | keep {retention} week(s)"
    if schedule_type == "monthly":
        return f"Monthly on day {details.get('day', '1')} at {time_value} | keep {retention} month(s)"
    if schedule_type == "yearly":
        month_names = {
            "1": "January",
            "2": "February",
            "3": "March",
            "4": "April",
            "5": "May",
            "6": "June",
            "7": "July",
            "8": "August",
            "9": "September",
            "10": "October",
            "11": "November",
            "12": "December",
        }
        return f"Yearly on {month_names.get(details.get('month', '1'), 'January')} {details.get('day', '1')} at {time_value} | keep {retention} year(s)"
    return schedule_type

def parse_time(time_value):
    hour, minute = (time_value or "00:00").split(":", 1)
    return hour.zfill(2), minute.zfill(2)


def matches_schedule(schedule_type, details, dt):
    hour, minute = parse_time(details.get("time", "00:00"))
    if dt.hour != int(hour) or dt.minute != int(minute):
        return False

    if schedule_type == "daily":
        return True
    
    if schedule_type == "weekly":
        schedule_day = int(details.get("day", "0"))
        python_weekday = dt.weekday()
        cron_weekday = (python_weekday + 1) % 7
        
        # DEBUG: Print what's happening
        print(f"🔍 DEBUG matches_schedule:")
        print(f"   schedule_day: {schedule_day} ({['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'][schedule_day]})")
        print(f"   python_weekday: {python_weekday} ({['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'][python_weekday]})")
        print(f"   cron_weekday: {cron_weekday} ({['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'][cron_weekday]})")
        print(f"   Match: {cron_weekday == schedule_day}")
        
        return cron_weekday == schedule_day
    
    if schedule_type == "monthly":
        return dt.day == int(details.get("day", "1"))
    
    if schedule_type == "yearly":
        return dt.month == int(details.get("month", "1")) and dt.day == int(details.get("day", "1"))
    
    return False


def apply_fallback_retention(config, fallback_retention):
    fallback = str(fallback_retention or "1")

    for schedule_type in SCHEDULE_TYPES:
        if not config[schedule_type].get("retention"):
            config[schedule_type]["retention"] = fallback

    enabled_types = [schedule_type for schedule_type in SCHEDULE_TYPES if config[schedule_type].get("enabled")]
    if fallback_retention is not None:
        for schedule_type in enabled_types:
            if schedule_type in config and not config[schedule_type].get("retention"):
                config[schedule_type]["retention"] = fallback


def get_schedule_retention(config, schedule_type, fallback_retention=1):
    normalized = normalize_schedule_config(config, fallback_retention=fallback_retention)
    details = normalized.get(schedule_type, {})
    try:
        return int(details.get("retention", fallback_retention))
    except (TypeError, ValueError):
        return int(fallback_retention)
