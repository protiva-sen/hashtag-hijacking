import json
import os
import sqlite3
import argparse
from youtubeapi import YouTubeAPI
from googleapiclient.errors import HttpError
from datetime import datetime
from utils import generate_time_ranges
from tqdm import tqdm
import pandas as pd

DEFAULT_DB_FILE = "/netfiles/compethicslab/hashtag-hijacking/youtube.db"
TRANSCRIPT_STATE_FILE = "/netfiles/compethicslab/hashtag-hijacking/state_transcripts.json"
TRANSCRIPT_CSV_LOCAL_FILE  = "transcripts.csv"

DELTAHOURS  = 6  # hours
def get_quries(search_queries_file):
    with open(search_queries_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def load_state():
    if os.path.exists(TRANSCRIPT_STATE_FILE):
        with open(TRANSCRIPT_STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(TRANSCRIPT_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def is_done(state, query, video_id):
    return query in state and video_id in state[query]

def mark_done(state, query, video_id):
    if query not in state:
        state[query] = {}
    state[query][video_id] = "done"  

def fetch_incomplete(start_date, end_date, query):
    # input = Database + Search parameters
    # output = Incomplete queries + hours

    incomplete = []
    if not os.path.exists(TRANSCRIPT_STATE_FILE):
        return [(query, start, end)
                for start, end in generate_time_ranges(start_date, end_date, deltahours=DELTAHOURS)]
    
    with open(TRANSCRIPT_STATE_FILE, 'r') as f:
        state = json.load(f)    

    for start, end in generate_time_ranges(start_date, end_date, deltahours=DELTAHOURS):
        start_str = start.isoformat()
        if not (query in state and start_str in state[query]):
            incomplete.append((query, start, end))

    return incomplete

    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--search_queries_file", type=str, required=True, help="Path to the file containing search queries")
    parser.add_argument("--start_date", type=str, required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end_date", type=str, required=True, help="End date in YYYY-MM-DD format")
    parser.add_argument("--DEBUG", action='store_true', help="Run in debug mode with fewer queries")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date)
    end_date = datetime.strptime(args.end_date)
    queries = get_quries(args.search_queries_file)

    with open('config.json', 'r') as f:
        config = json.load(f)
        api_key = config.get['API_KEY']

    yt_api = YouTubeAPI(api_key)
    conn = sqlite3.connect(DEFAULT_DB_FILE)
    state = load_state()

    incomplete_queries = fetch_incomplete(start_date, end_date, queries[0])

    if args.DEBUG:
        incomplete_queries = incomplete_queries[:1]
        print("DEBUG mode: Only processing the first incomplete query.")

    transcripts_data = []

    for query, start, end in tqdm(incomplete_queries):
        print(f"Processing query '{query}' from {start} to {end}")
        
        videos = yt_api.fetch_data(query, start, end)
        for video in videos:
            video_id = video['id']
            if is_done(state, query, video_id):
                continue
            try:
                transcript = yt_api.fetch_transcript(video_id)
                transcript_df = pd.DataFrame([{
                    "video_id": video_id,
                        "query": query,
                        "transcript": transcript
                    }])
                transcript_df.to_csv(TRANSCRIPT_CSV_LOCAL_FILE, mode='a', header=not os.path.exists(TRANSCRIPT_CSV_LOCAL_FILE), index=False)    

                transcripts_data.append({
                        "video_id": video_id,
                        "query": query,
                        "transcript": transcript
                    })
                mark_done(state, query, video_id)
                save_state(state)

            except HttpError as e:
                print(f"Failed to fetch transcript for video {video_id}: {e}")
                with open("error_caption.txt", "a") as log_file:
                    log_file.write(f"Error fetching transcript for video {video_id}: {e}\n")
                save_state(state)
            except Exception as e:
                print(f"Unexpected error for video {video_id}: {e}")
                with open("error_caption.txt", "a") as log_file:
                    log_file.write(f"Error fetching transcript for video {video_id}: {e}\n")
                save_state(state)  
                continue
    if transcripts_data:
        final_df = pd.DataFrame(transcripts_data)
        final_df.to_csv(TRANSCRIPT_CSV_LOCAL_FILE, mode='a', header=not os.path.exists(TRANSCRIPT_CSV_LOCAL_FILE), index=False)

    conn.close()

if __name__ == "__main__":
    main()


                