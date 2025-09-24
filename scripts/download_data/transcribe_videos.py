import whisper
from tqdm import tqdm
import sqlite3
import argparse

DEFAULT_DB_FILE = "/netfiles/compethicslab/hashtag-hijacking/youtube.db"
SATE_FILE = "" # new state file for transcribe

def get_queries():
    pass

def load_state():
    pass

def save_state(state):
    pass

def is_done():
    pass

def mark_done():
    pass

def fetch_incomplete():
    pass

def get_video_ids():
    pass

def get_video_audio():
    pass #use youtube-dl

def save_text_to_sql():
    pass

def delete_temp_audios():
    pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_name', type=str, required=True, help="Model needed for whisper")
    parser.add_argument('--query', type=str, required=True, help="Hashtag needed to run")
    # add for start and end date too

    args = parser.parse_args()
    model_name = args.model_name

    model = whisper.load_model(model_name)
    state = load_state()
    conn = sqlite3.connect(DEFAULT_DB_FILE)

    incomplete_queries = fetch_incomplete(start_date, end_date, query)

    for query, start, end in tqdm(incomplete_queries):
        try:
            video_ids = get_video_ids(query, start, end)
            
            if not video_ids:
                print("No video IDs for query...")
                
                mark_done(state, query, start)

            for video_id in video_ids:
                audio_path = get_video_audio():
                text = model.transcribe(audio)
                save_text_to_sql(text)

