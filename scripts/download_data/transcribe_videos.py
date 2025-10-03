import whisper
from tqdm import tqdm
import sqlite3
import argparse
import os
import json
import yt_dlp
from datetime import datetime, timedelta


DEFAULT_DB_FILE = "/netfiles/compethicslab/hashtag-hijacking/youtube.db"
TRANS_STATE_FILE = "transcribe_videos_state.json"
Temp_dir = "temp_audios"

os.makedirs(Temp_dir, exist_ok=True)
    

def load_state():
    if os.path.exists(TRANS_STATE_FILE):
        with open(TRANS_STATE_FILE, 'r') as f:
            return json.load(f)
    else:
        return {"done": []}   

def save_state(state):
    with open(TRANS_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2) 

def is_done(state, query, start):
    return (query, start) in state["done"]
    

def mark_done(state, query, start):
    if (query, start) not in state:
        state["done"].append((query, start))
        save_state(state)
    

def fetch_incomplete(query, start_date, end_date):
    incomplete = []

    if not os.path.exists(TRANS_STATE_FILE):
        return [(query, start_date.isoformat(), end_date.isoformat())]
    
    state = load_state()

    for entry in state["done"]:
        if entry[0] == query and entry[1] == start_date.isoformat():
            print(f"{query} {start_date} as already done")
            return []
    incomplete.append((query, start_date.isoformat(), end_date.isoformat()))

    return incomplete  

def get_video_ids(conn):
    query = 'SELECT DISTINCT video_id FROM youtube_videos'
    return [row[0] for row in conn.execute(query).fetchall()]
    

def get_video_audio(video_id):
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': os.path.join(Temp_dir, f'{video_id}.%(ext)s'),
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'quiet': True,
        'no_warnings': True,
    }
    
    url = f"https://www.youtube.com/watch?v={video_id}"
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])
    
    audio_path = os.path.join(Temp_dir, f"{video_id}.mp3")
    if os.path.exists(audio_path):
        return audio_path
    else:
        return None

def save_text_to_sql(conn, video_id, text):
    query = 'INSERT OR REPLACE INTO video_transcripts (video_id, transcript) VALUES (?, ?)'
    conn.execute(query, (video_id, text))
    conn.commit()

def delete_temp_audios():
    for file in os.listdir(Temp_dir):
        file_path = os.path.join(Temp_dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_name', type=str, required=True, help="Model needed for whisper")
    parser.add_argument('--query', type=str, required=True, help="Hashtag needed to run")
    parser.add_argument('--start_date', type=str, required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument('--end_date', type=str, required=True, help="End date in YYYY-MM-DD")
    parser.add_argument('--db_file', type=str, default=DEFAULT_DB_FILE, help="Path to SQLite database file")    
    args = parser.parse_args()
    model_name = args.model_name

    model = whisper.load_model(model_name)
    state = load_state()
    conn = sqlite3.connect(DEFAULT_DB_FILE)

    incomplete_queries = fetch_incomplete(args.query, args.start_date, args.end_date)

    for query, start, end in tqdm(incomplete_queries):
        if is_done(state, query, start):
            continue

        try: 
            video_ids = get_video_ids(conn)
            
            if not video_ids:
                mark_done(state, query, start)
                continue

            for video_id in tqdm(video_ids, desc=f"Processing videos for {query} from {start} to {end}"):
                audio_path = get_video_audio(video_id)
                if not audio_path:
                    print(f"Failed to download audio for video {video_id}")
                    continue
                text = model.transcribe(audio_path)["text"]
                save_text_to_sql(conn, video_id, text)
                os.remove(audio_path)

            mark_done(state, query, start)
            delete_temp_audios()
        except Exception as e:
            print(f"Error processing {query} from {start} to {end}: {e}")
            continue
    conn.close()
if __name__ == "__main__":  
    main()
       

