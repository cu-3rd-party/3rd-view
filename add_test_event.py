# Файл: scripts/add_test_event.py
import sys
import os

# Добавляем корень проекта в PATH, чтобы видеть config и database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db_connection

def add_test_event():
    event_id = "test_manual_event_postgres_001"
    start_time = "2026-04-16T23:45:00+03:00"
    end_time = "2026-04-16T23:47:00+03:00"
    instance_start_ts = "2026-04-16T20:45:00.000Z"
    
    event_name = "Дискретная математика, последняя тестовая пара (PG)"
    link_description = "https://centraluniversity.ktalk.ru/w33jrhh8aprj"
    attendees_emails = ""
    teacher_names = "Тестовый Преподаватель Postgres"
    total_attendees = 1

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # В PostgreSQL используем INSERT ... ON CONFLICT DO UPDATE вместо SQLite REPLACE
            cur.execute("""
                INSERT INTO calendar_events 
                (event_id, instance_start_ts, start_time, end_time, event_name, 
                 total_attendees, link_description, attendees_emails, teacher_names)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id, instance_start_ts) DO UPDATE SET
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    event_name = EXCLUDED.event_name,
                    total_attendees = EXCLUDED.total_attendees,
                    link_description = EXCLUDED.link_description,
                    attendees_emails = EXCLUDED.attendees_emails,
                    teacher_names = EXCLUDED.teacher_names
            """, (event_id, instance_start_ts, start_time, end_time, event_name, 
                  total_attendees, link_description, attendees_emails, teacher_names))
        conn.commit()
        print(f"✅ Тестовая пара успешно добавлена в базу PostgreSQL!")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    add_test_event()