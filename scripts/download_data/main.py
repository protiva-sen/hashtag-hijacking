import argparse
from datetime import datetime
from utils import generate_hourly_ranges
from youtubeapi import YouTubeAPI
import json
import pandas as pd
import sqlite3
import os

STATE_FILE = "state.json"
DEFAULT_DB_FILE = "youtube.db"

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

def get_quries(search_queries_file):
    with open(search_queries_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]
    
def fetch_incomplete(start_date, end_date, queries):
    # input = Database + Search parameters
    # output = Incomplete queries + hours

    incomplete = []
    if not os.path.exists(STATE_FILE):
        return [(query, start, end) for query in queries
                for start, end in generate_hourly_ranges(start_date, end_date)]

    with open(STATE_FILE, 'r') as f:
        state = json.load(f)

    for query in queries:
        for start, end in generate_hourly_ranges(start_date, end_date):
            start_str = start.isoformat()
            if not (query in state and start_str in state[query]):
                incomplete.append((query, start, end))

    return incomplete

# create a database of queries that need to be run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, required=True, help="Start date in format YYYY-MM-DDTHH e.g. 2023-01-01T00:00")
    parser.add_argument('--end_date', type=str, required=True,  help="End date in format YYYY-MM-DDTHH e.g. 2023-01-01T23:59")
    parser.add_argument('--queries', type=str, required=True, help="text file with search queries, one per line")
    parser.add_argument('--api_key', type=str, required=False, help="YouTube API key to override config.json")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%dT%H:%M")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%dT%H:%M")
    search_queries_file = args.queries
    SEARCH_QUERIES = get_quries(search_queries_file)

    if args.api_key:
        api_key = args.api_key
    else:
        with open('config.json', 'r') as f:
            config = json.load(f)
            api_key = config.get('API_KEY')

    # make all data queries in youtube_db
    # get all queries in youtube_db

    youtube_api = YouTubeAPI(config['API_KEY'])
    state = load_state()
    conn = sqlite3.connect(DEFAULT_DB_FILE)

    incomplete_queries = fetch_incomplete(start_date, end_date, SEARCH_QUERIES)

    for query, start, end in incomplete_queries:
        start_str = start.isoformat()

       
        try:
            videos = youtube_api.fetch_data(query, start, end)
            if not videos:
                print(f"No videos found for query '{query}' from {start_str} to {end.isoformat()}")
                mark_done(state, query, start_str)
                continue

            videos_df = pd.DataFrame(videos)
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
            query = youtube_api.curr_query
            start = youtube_api.curr_start
            end = youtube_api.curr_end
            page_num = youtube_api.curr_page
            print(f"Error fetching data for query '{query}' from {start_str} to {end.isoformat()}: {e}")
            with open('error_log.txt', 'a') as error_file:
                error_file.write(f"Error for query '{query}' from {start_str} to {end.isoformat()}: {e}\n")
            
            continue


if __name__ == "__main__":
    main()