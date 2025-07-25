from datetime import datetime, timezone, timedelta
import json
import os

# STATE_FILE = "state.json"


def generate_time_ranges(start_date, end_date, deltahours):
    current = start_date
    while current < end_date:
        yield current, current + timedelta(hours=deltahours)
        current += timedelta(hours=deltahours)

def to_rfc3339(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace('+00:00', 'Z')

def clean_text(text):
    return text.replace('\n', ' ').replace('\r', ' ').strip()