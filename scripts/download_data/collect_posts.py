import argparse
from datetime import datetime
from utils import generate_time_ranges
from youtubeapi import YouTubeAPI
import json
import pandas as pd
import sqlite3
import os
from tqdm import tqdm

STATE_FILE = "/netfiles/compethicslab/hashtag-hijacking/state.json"
DEFAULT_DB_FILE = "/netfiles/compethicslab/hashtag-hijacking/youtube.db"
DELTAHOURS = 24

def get_queries(search_queries_file):
    with open(search_queries_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def is_done(state, query, start_str):
    return query in state and start_str in state[query]

def mark_done(state, query, start_str):
    if query not in state:
        state[query] = {}
    state[query][start_str] = "done"
    
def fetch_incomplete(start_date, end_date, query):
    # input = Database + Search parameters
    # output = Incomplete queries + hours

    incomplete = []
    if not os.path.exists(STATE_FILE):
        return [(query, start, end)
                for start, end in generate_time_ranges(start_date, end_date, deltahours=DELTAHOURS)]

    with open(STATE_FILE, 'r') as f:
        state = json.load(f)

    for start, end in generate_time_ranges(start_date, end_date, deltahours=DELTAHOURS):
        start_str = start.isoformat()
        if not (query in state and start_str in state[query]):
            incomplete.append((query, start, end))

    return incomplete

# create a database of queries that need to be run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, required=True, help="Start date in format YYYY-MM-DDTHH e.g. 2023-01-01T00:00")
    parser.add_argument('--end_date', type=str, required=True,  help="End date in format YYYY-MM-DDTHH e.g. 2023-01-01T23:59")
    parser.add_argument('--query', type=str, required=True, help="query need to be run")
    parser.add_argument('--DEBUG', action='store_true', help="Run in debug mode with fewer queries")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%dT%H:%M")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%dT%H:%M")
    query = args.query
    
    with open('config.json', 'r') as f:
        config = json.load(f)
        api_key = config.get('API_KEY')

    # make all data queries in youtube_db
    # get all queries in youtube_db

    youtube_api = YouTubeAPI(api_key)
    state = load_state()
    conn = sqlite3.connect(DEFAULT_DB_FILE)

    incomplete_queries = fetch_incomplete(start_date, end_date, query)

    for query, start, end in tqdm(incomplete_queries):
        start_str = start.isoformat()

        try:
            # fix such that if there is a quota error or any other error, it will skip
            videos = youtube_api.fetch_data(query, start, end)
            if not videos:
                print(f"No videos found for query '{query}' from {start_str} to {end.isoformat()}", flush=True)
                mark_done(state, query, start_str)
                continue

            videos_df = pd.DataFrame(videos)
            
            if args.DEBUG:
                print(f"DEBUG: Fetched {len(videos_df)} videos for query '{query}' from {start_str} to {end.isoformat()}", flush=True)

            videos_df.to_sql(
                'youtube_videos',
                con=conn,
                if_exists='append',
                index=False
            )
            mark_done(state, query, start_str)
            save_state(state)  # save the state after each successful query
            # push to database

        except Exception as e:

            failed_query = youtube_api.curr_query or query
            failed_start = youtube_api.curr_start or start
            failed_end = youtube_api.curr_end or end
            page_num = youtube_api.curr_page or "?"

            err_msg = str(e).lower()
            if "quota" in err_msg or "403" in err_msg:
                print(f"[Quota Error] Skipping query '{failed_query}' from {failed_start} to {failed_end}", flush=True)
            else:
                print(f"[Error] Query '{failed_query}' from {failed_start} to {failed_end} (page {page_num}): {e}", flush=True)
            with open('error_log.txt', 'a') as error_file:
                error_file.write(f"Error for query '{failed_query}' from {failed_start.isoformat()} to {failed_end.isoformat()} (page {page_num}): {e}\n")  

            start_str_log = failed_start.isoformat() if hasattr(failed_start, "isoformat") else str(failed_start)
            end_str_log = failed_end.isoformat() if hasattr(failed_end, "isoformat") else str(failed_end) 

            # Log the error with context
            with open('error_log.txt', 'a') as error_file:
                 error_file.write(
                        f"Error for query '{failed_query}' from {start_str_log} to {end_str_log} (page {page_num}): {e}\n")  
            
            
            # Log the error with query and time range
            youtube_api.curr_query = query
            youtube_api.curr_start = start
            youtube_api.curr_end = end
            # youtube_api.curr_page = 1  # Reset page number for the next attempt

            save_state(state)  # save the state even if there is an error                 
            
            continue


if __name__ == "__main__":
    main()