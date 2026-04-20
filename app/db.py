from contextlib import contextmanager

import psycopg2
from psycopg2.extensions import connection as PgConnection

from app.core.config import get_settings


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS courses (
    id SERIAL PRIMARY KEY,
    track TEXT,
    course_year TEXT,
    level TEXT,
    name TEXT,
    search_query TEXT,
    link TEXT
);
CREATE TABLE IF NOT EXISTS teachers (
    email TEXT PRIMARY KEY,
    full_name TEXT
);
CREATE TABLE IF NOT EXISTS students (
    email TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS student_calendar_link (
    student_email TEXT,
    event_id TEXT,
    instance_start_ts TEXT,
    PRIMARY KEY (student_email, event_id, instance_start_ts)
);
CREATE TABLE IF NOT EXISTS calendar_events (
    event_id TEXT,
    instance_start_ts TEXT,
    start_time TEXT,
    end_time TEXT,
    event_name TEXT,
    total_attendees INTEGER,
    link_description TEXT,
    attendees_emails TEXT,
    teacher_names TEXT,
    PRIMARY KEY (event_id, instance_start_ts)
);
CREATE TABLE IF NOT EXISTS event_recordings (
    yandex_event_id TEXT,
    yandex_instance_start_ts TEXT,
    ktalk_id TEXT,
    recording_url TEXT,
    transcription_url TEXT,
    is_manual BOOLEAN DEFAULT FALSE,
    recording_date TEXT,
    bot_visited_at TEXT,
    PRIMARY KEY (yandex_event_id, yandex_instance_start_ts)
);
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE,
    password_hash TEXT,
    is_active BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS email_verifications (
    email TEXT PRIMARY KEY,
    code TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def get_db_connection() -> PgConnection:
    settings = get_settings()
    return psycopg2.connect(**settings.db_config)


@contextmanager
def db_connection() -> PgConnection:
    conn = get_db_connection()
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
