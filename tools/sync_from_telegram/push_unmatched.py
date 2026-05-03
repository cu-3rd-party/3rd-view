import os
import sys
import datetime
from zoneinfo import ZoneInfo
import re
import psycopg2
import psycopg2.extras

if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# Импортируем нужные функции из sync_records
from sync_records import load_ktalk_token, get_ktalk_record_info, DB_CONFIG

def get_existing_urls(conn):
    existing = set()
    with conn.cursor() as cur:
        cur.execute("SELECT recording_url FROM event_recordings WHERE recording_url IS NOT NULL")
        existing.update(row[0] for row in cur.fetchall())
        
        # Проверяем suggested_recordings (в try/except, вдруг таблицы нет)
        try:
            cur.execute("SELECT suggested_url FROM suggested_recordings WHERE suggested_url IS NOT NULL")
            existing.update(row[0] for row in cur.fetchall())
        except psycopg2.Error:
            conn.rollback()

        # Проверяем unmatched_recordings
        try:
            cur.execute("SELECT recording_url FROM unmatched_recordings WHERE recording_url IS NOT NULL")
            existing.update(row[0] for row in cur.fetchall())
        except psycopg2.Error:
            conn.rollback()
            
    return existing

def push_unmatched(parsed_file):
    print("🚀 Начинаем добавление непривязанных записей...")
    
    try:
        with open(parsed_file, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"❌ Файл {parsed_file} не найден!")
        return

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return
        
    # Создаем таблицу, если её нет
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS unmatched_recordings (
                id SERIAL PRIMARY KEY,
                ktalk_id TEXT UNIQUE,
                title TEXT,
                start_time TEXT,
                recording_url TEXT,
                room_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

    existing_urls = get_existing_urls(conn)
    print(f"📊 Найдено {len(existing_urls)} уже известных ссылок в БД (привязанных/предложенных/отложенных).")

    new_urls = [u for u in urls if u not in existing_urls]
    print(f"🔍 К добавлению найдено {len(new_urls)} новых ссылок из {len(urls)} всего.")

    if not new_urls:
        print("✅ Нет новых непривязанных ссылок для добавления.")
        conn.close()
        return

    msk_zone = ZoneInfo("Europe/Moscow")
    added_count = 0

    with conn.cursor() as cur:
        for url in new_urls:
            ktalk_id = url.split('/')[-1]
            record_data = get_ktalk_record_info(ktalk_id)
            
            if not record_data:
                print(f"❌ Не удалось получить данные API для {url}")
                continue
                
            ktalk_title = record_data.get('title', '')
            created_date_str = record_data.get('createdDate')
            room_name = record_data.get('space_name') or record_data.get('room_name') or ''
            
            try:
                # Отрезаем доли секунд
                clean_date_str = re.sub(r'\.\d+', '', created_date_str).replace("Z", "+00:00")
                dt_utc = datetime.datetime.fromisoformat(clean_date_str)
                dt_msk = dt_utc.astimezone(msk_zone)
                iso_time = dt_msk.isoformat()
            except Exception as e:
                print(f"⚠️ Ошибка даты для {url}: {e}")
                iso_time = created_date_str

            try:
                cur.execute("""
                    INSERT INTO unmatched_recordings (ktalk_id, title, start_time, recording_url, room_name)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (ktalk_id) DO NOTHING
                """, (ktalk_id, ktalk_title, iso_time, url, room_name))
                conn.commit()
                added_count += 1
                print(f"✅ Добавлено: {ktalk_title} ({iso_time}) -> {url}")
            except Exception as e:
                conn.rollback()
                print(f"❌ Ошибка вставки в БД для {url}: {e}")

    conn.close()
    print(f"\n🎉 Готово! Успешно добавлено {added_count} непривязанных записей.")

if __name__ == "__main__":
    push_unmatched('parsed_records.txt')
