from datetime import datetime, timezone, timedelta
import json
import os

STATE_FILE = "state.json"


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

def clean_text(text):
    return text.replace('\n', ' ').replace('\r', ' ').strip()

def save_state(query, start):
    state = {"last_query": query, "last_start": start.isoformat()}
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return None