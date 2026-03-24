import argparse
import json
import os
import sqlite3
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from utils import generate_time_ranges
from youtubeapi import YouTubeAPI

STATE_FILE = "/netfiles/compethicslab/hashtag-hijacking/state_hashtags.json"
DEFAULT_DB_FILE = "/netfiles/compethicslab/hashtag-hijacking/youtube_hashtags.db"
DELTAHOURS = 1

SEEDS = [
    "the", "and", "to", "a", "is",
    "in", "of", "for", "my", "new",
    "how", "why", "best", "today", "live",
]


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)


def fetch_incomplete(start_date, end_date, seeds):
    """
    Return (seed, start, end) tuples that haven't been marked done yet,
    across all seeds × all time windows.
    """
    incomplete = []

    if not os.path.exists(STATE_FILE):
        for seed in seeds:
            for start, end in generate_time_ranges(start_date, end_date, deltahours=DELTAHOURS):
                incomplete.append((seed, start, end))
        return incomplete

    with open(STATE_FILE, 'r') as f:
        state = json.load(f)

    for seed in seeds:
        for start, end in generate_time_ranges(start_date, end_date, deltahours=DELTAHOURS):
            start_str = start.isoformat()
            if not (seed in state and start_str in state.get(seed, {})):
                incomplete.append((seed, start, end))

    return incomplete


def mark_done(state, seed, start_str):
    if seed not in state:
        state[seed] = {}
    state[seed][start_str] = "done"


def ensure_hashtags_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hashtags (
            seed        TEXT,
            video_id    TEXT,
            hashtag     TEXT
        )
    """)
    conn.commit()



def main():
    parser = argparse.ArgumentParser(
        description="Collect hashtags from YouTube videos matching seed queries and time windows.")
    parser.add_argument(
        '--start_date', type=str, required=True,
        help="Start date in format YYYY-MM-DDTHH:MM  e.g. 2023-01-01T00:00"
    )
    parser.add_argument(
        '--end_date', type=str, required=True,
        help="End date in format YYYY-MM-DDTHH:MM  e.g. 2023-01-31T23:59"
    )
    parser.add_argument(
        '--DEBUG', action='store_true',
        help="Print extra info during run"
    )
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%dT%H:%M")
    end_date   = datetime.strptime(args.end_date,   "%Y-%m-%dT%H:%M")

    with open('config.json', 'r') as f:
        config = json.load(f)
    api_key = config.get('API_KEY')

    youtube_api = YouTubeAPI(api_key)
    state = load_state()

    conn = sqlite3.connect(DEFAULT_DB_FILE)
    ensure_hashtags_table(conn)

    incomplete = fetch_incomplete(start_date, end_date, SEEDS)
    print(f"[INFO] {len(incomplete)} (seed, window) pairs remaining across {len(SEEDS)} seeds.")

    for seed, start, end in tqdm(incomplete, desc="Collecting hashtags"):
        start_str = start.isoformat()

        try:
            rows = youtube_api.fetch_hashtags(seed, start, end)

            if not rows:
                if args.DEBUG:
                    print(f"[DEBUG] No hashtags for seed='{seed}' {start_str} → {end.isoformat()}")
                mark_done(state, seed, start_str)
                save_state(state)
                continue

            df = pd.DataFrame(rows, columns=["seed", "video_id", "hashtag"])

            if args.DEBUG:
                print(
                    f"[DEBUG] {len(df)} rows  seed='{seed}'  "
                    f"{start_str} → {end.isoformat()}"
                )

            df.to_sql('hashtags', con=conn, if_exists='append', index=False)
            mark_done(state, seed, start_str)
            save_state(state)

        except Exception as e:
            failed_seed  = youtube_api.curr_query or seed
            failed_start = youtube_api.curr_start or start
            failed_end   = youtube_api.curr_end   or end
            page_num     = youtube_api.curr_page  or "?"

            err_msg = str(e).lower()
            if "quota" in err_msg or "403" in err_msg:
                print(f"[Quota Error] Skipping seed='{failed_seed}' {failed_start} → {failed_end}", flush=True)
            else:
                print(f"[Error] seed='{failed_seed}' {failed_start} → {failed_end} (page {page_num}): {e}", flush=True)

            start_str_log = failed_start.isoformat() if hasattr(failed_start, "isoformat") else str(failed_start)
            end_str_log   = failed_end.isoformat()   if hasattr(failed_end,   "isoformat") else str(failed_end)

            with open('error_hashtags.txt', 'a') as logf:
                logf.write(
                    f"seed='{failed_seed}' from {start_str_log} to "
                    f"{end_str_log} (page {page_num}): {e}\n"
                )

            youtube_api.curr_query = seed
            youtube_api.curr_start = start
            youtube_api.curr_end   = end
            youtube_api.curr_page  = None

            save_state(state)
            continue

    conn.close()
    print("[INFO] Done.")


if __name__ == "__main__":
    main()