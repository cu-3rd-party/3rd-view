import psycopg2
DB_CONFIG = {'dbname':'cu_view_db', 'user':'cu_view_user', 'password':'CmvJBTkVJ7Pk', 'host':'87.242.85.13', 'port':'5432'}
with psycopg2.connect(**DB_CONFIG) as conn:
    with conn.cursor() as cur:
        cur.execute("""
CREATE TABLE IF NOT EXISTS suggested_recordings (
    id SERIAL PRIMARY KEY,
    yandex_event_id TEXT,
    yandex_instance_start_ts TEXT,
    suggested_url TEXT,
    suggested_by_email TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
        """)
    conn.commit()
    print("Table created!")
