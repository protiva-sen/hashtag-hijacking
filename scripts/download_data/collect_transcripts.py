import json
import os
import sqlite3
import argparse
from youtube_transcript_api import YouTubeTranscriptApi
from youtubeapi import YouTubeAPI
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound, VideoUnavailable, CouldNotRetrieveTranscript
from tqdm import tqdm
import pandas as pd

TRANSCRIPT_STATE_FILE = "/netfiles/compethicslab/hashtag-hijacking/state_transcripts.json"
DEFAULT_DB_FILE = "/netfiles/compethicslab/hashtag-hijacking/youtube.db"
TRANSCRIPT_CSV_FILE_LOCAL = "youtube_transcripts.csv"

def load_state():
    if os.path.exists(TRANSCRIPT_STATE_FILE):
        with open(TRANSCRIPT_STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(TRANSCRIPT_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def get_video_ids(conn):
    query = "SELECT DISTINCT video_id FROM youtube_videos"
    return [row[0] for row in conn.execute(query).fetchall()]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--DEBUG', action='store_true', help="Debug mode: limit number of videos")
    args = parser.parse_args()

    with open('config.json', 'r') as f:
        config = json.load(f)
    api_key = config.get("API_KEY")

    conn = sqlite3.connect(DEFAULT_DB_FILE)
    video_ids = get_video_ids(conn)
    state = load_state()
    api = YouTubeAPI(api_key)

    if args.DEBUG:
        print(f"[DEBUG] Total videos to process: {len(video_ids)}")

    transcripts_data = []

    for video_id in tqdm(video_ids, desc="Fetching transcripts"):
        if state.get(video_id) == "done":
            continue

        try:
            transcript,language = api.fetch_transcript(video_id)
            transcript_text = " ".join([entry['text'] for entry in transcript])

        except NoTranscriptFound:
            state[video_id] = "error: no transcript found"
            save_state(state)
            continue
        except TranscriptsDisabled:
            state[video_id] = "error: transcripts disabled"
            save_state(state)
            continue
        except VideoUnavailable:
            state[video_id] = "error: video unavailable"
            save_state(state)
            continue
        except CouldNotRetrieveTranscript:
            state[video_id] = "error: could not retrieve transcript"
            save_state(state)
            continue
        except Exception as e:
            print(f"[ERROR] {video_id}: {e}")
            with open("error_transcripts.txt", "a") as logf:
                logf.write(f"{video_id}: {str(e)}\n")
            save_state(state)
            continue

        
        transcript_df  = pd.DataFrame([{
            "video_id": video_id,
            "transcript_text": transcript_text,
            "language": language
        }])
        transcript_df.to_sql('youtube_transcripts', con=conn, if_exists='append', index=False)  

        transcripts_data.append({
            "video_id": video_id,
            "transcript_text": transcript_text,
            "language": language
        })

        state[video_id] = "done"
        save_state(state)   

        if transcripts_data:
            final_df = pd.DataFrame(transcripts_data)
            final_df.to_csv(TRANSCRIPT_CSV_FILE_LOCAL, index=False)

    conn.close()

if __name__ == "__main__":
    main()  



