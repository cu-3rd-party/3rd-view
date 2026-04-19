# Файл: database.py
import psycopg2
from psycopg2.extras import RealDictCursor
from config import Config

def get_db_connection():
    return psycopg2.connect(**Config.DB_CONFIG)

def init_db():
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS courses (
                id SERIAL PRIMARY KEY, track TEXT, course_year TEXT, 
                level TEXT, name TEXT, search_query TEXT, link TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS teachers (
                email TEXT PRIMARY KEY, full_name TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS students (
                email TEXT PRIMARY KEY
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS student_calendar_link (
                student_email TEXT, event_id TEXT, instance_start_ts TEXT,
                PRIMARY KEY (student_email, event_id, instance_start_ts)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS calendar_events (
                event_id TEXT, instance_start_ts TEXT, start_time TEXT, 
                end_time TEXT, event_name TEXT, total_attendees INTEGER, 
                link_description TEXT, attendees_emails TEXT, teacher_names TEXT,
                PRIMARY KEY (event_id, instance_start_ts)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS event_recordings (
                yandex_event_id TEXT, yandex_instance_start_ts TEXT, 
                ktalk_id TEXT, recording_url TEXT, transcription_url TEXT, 
                is_manual BOOLEAN DEFAULT FALSE, recording_date TEXT, 
                bot_visited_at TEXT,
                PRIMARY KEY (yandex_event_id, yandex_instance_start_ts)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY, email TEXT UNIQUE, password_hash TEXT, 
                is_active BOOLEAN DEFAULT FALSE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS email_verifications (
                email TEXT PRIMARY KEY, code TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    conn.commit()
    conn.close()

# При импорте модуля таблицы будут созданы/проверены
init_db()