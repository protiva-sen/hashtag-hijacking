from datetime import datetime, timedelta, timezone

def generate_hourly_ranges(start_date, end_date):
    current = start_date
    while current < end_date:
        yield current, current + timedelta(hours=1)
        current += timedelta(hours=1)

def to_rfc3339(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace('+00:00', 'Z')
