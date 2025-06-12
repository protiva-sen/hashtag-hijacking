import sqlite3

def init_db():
    conn = sqlite3.connect("youtube_data.db")
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            video_id TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            published_at TEXT,
            channel_id TEXT,
            channel_title TEXT,
            tags TEXT,
            category_id TEXT,
            default_audio_language TEXT,
            default_language TEXT,
            duration TEXT,
            dimension TEXT,
            definition TEXT,
            caption TEXT,
            licensed_content BOOLEAN,
            projection TEXT,
            view_count INTEGER,
            like_count INTEGER,
            favorite_count INTEGER,
            comment_count INTEGER,
            channel_custom_url TEXT,
            channel_published_at TEXT,
            channel_view_count INTEGER,
            channel_subscriber_count INTEGER,
            channel_hidden_subscribers TEXT,
            channel_video_count INTEGER,
            channel_country TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT PRIMARY KEY,
            video_id TEXT,
            text TEXT,
            author TEXT,
            published_at TEXT
        )
    ''')
    conn.commit()
    return conn
