import os
import datetime
from zoneinfo import ZoneInfo
import re
import requests
import psycopg2

# Твои данные БД
DB_CONFIG = {
    'dbname': 'cu_view_db',
    'user': 'cu_view_user',
    'password': 'SRY_NO_DOCKER_PASSWORD',
    'host': '127.0.0.1',
    'port': '5432'
}

def get_events_without_records_from_db():
    query = """
        SELECT c.event_id, c.instance_start_ts, c.start_time, c.event_name
        FROM calendar_events c
        LEFT JOIN event_recordings r 
          ON c.event_id = r.yandex_event_id AND c.instance_start_ts = r.yandex_instance_start_ts
        WHERE r.recording_url IS NULL;
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            
    events = []
    msk_zone = ZoneInfo("Europe/Moscow")
    
    for row in rows:
        event_id, instance_start_ts, start_time_str, event_name = row
        if not start_time_str:
            continue
            
        try:
            # Приводим время из БД к Москве
            dt = datetime.datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=msk_zone)
            else:
                dt = dt.astimezone(msk_zone)
                
            events.append({
                "event_id": event_id,
                "instance_start_ts": instance_start_ts,
                "start_time": dt,
                "event_name": event_name or ""
            })
        except Exception as e:
            print(f"⚠️ Ошибка парсинга даты из БД: {start_time_str} - {e}")
            
    return events

def update_event_record_in_db(event_id, instance_start_ts, ktalk_id, recording_url, recording_date_str, transcription_url):
    query = """
        INSERT INTO event_recordings (
            yandex_event_id, yandex_instance_start_ts, ktalk_id, 
            recording_url, transcription_url, is_manual, recording_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (yandex_event_id, yandex_instance_start_ts) 
        DO UPDATE SET 
            ktalk_id = EXCLUDED.ktalk_id,
            recording_url = EXCLUDED.recording_url,
            transcription_url = EXCLUDED.transcription_url,
            recording_date = EXCLUDED.recording_date;
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (
                event_id, instance_start_ts, ktalk_id, 
                recording_url, transcription_url, False, recording_date_str
            ))
        conn.commit()

def load_ktalk_token(auth_file="ktalk_auth.txt"):
    if not os.path.exists(auth_file):
        return None
    with open(auth_file, "r", encoding="utf-8") as f:
        token = f.read().strip()
    if not token.startswith("Session ") and not token.startswith("Bearer "):
        token = f"Session {token}" 
    return token

def get_ktalk_record_info(record_id):
    url = f"https://centraluniversity.ktalk.ru/api/recordings/{record_id}"
    token = load_ktalk_token()
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "X-Platform": "web",
    }
    if token:
        headers["Authorization"] = token

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 401:
            print(f"❌ Ошибка 401: Токен в ktalk_auth.txt протух!")
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"❌ Ошибка API Толка для {record_id}: {e}")
        return None

def clean_record_title(title):
    # Убираем слово "Запись" (регистронезависимо) и чистим пробелы по краям
    t = re.sub(r'(?i)запись', '', title).strip()
    return t

def sync_records(parsed_file):
    print("🚀 Начинаем строгую синхронизацию записей...")
    
    try:
        with open(parsed_file, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"❌ Файл {parsed_file} не найден!")
        return

    db_events = get_events_without_records_from_db()
    if not db_events:
        print("В базе нет пар без записей. Все синхронизировано!")
        return

    print(f"📊 Найдено пар без записей: {len(db_events)}. Обрабатываем {len(urls)} ссылок из Толка...")
    msk_zone = ZoneInfo("Europe/Moscow")

    success_count = 0
    
    log_results = {
        "success": [],
        "failed": []
    }

    for url in urls:
        ktalk_id = url.split('/')[-1]
        record_data = get_ktalk_record_info(ktalk_id)
        
        if not record_data:
            log_results["failed"].append(f"Не удалось получить данные для {url}")
            continue
            
        ktalk_title = record_data.get('title', '')
        created_date_str = record_data.get('createdDate')
        
        transcription_url = None
        if 'transcription' in record_data and isinstance(record_data['transcription'], dict):
            transcription_url = record_data['transcription'].get('transcriptionUrl')

        try:
            # Отрезаем доли секунд (всё что после точки до Z), чтобы Питон не давился
            clean_date_str = re.sub(r'\.\d+', '', created_date_str).replace("Z", "+00:00")
            dt_utc = datetime.datetime.fromisoformat(clean_date_str)
            dt_msk = dt_utc.astimezone(msk_zone)
        except Exception as e:
            print(f"⚠️ Ошибка даты для {ktalk_title}: {e}")
            log_results["failed"].append(f"Ошибка парсинга даты для записи: '{ktalk_title}' ({url}) | Ссылка: {url}")
            continue

        # Игнорируем записи после 13.04.2026
        if dt_msk.date() > datetime.date(2026, 4, 13):
            # print(f"⏭️ Пропуск записи после 13.04.2026: {ktalk_title}")
            continue

        clean_ktalk_title = clean_record_title(ktalk_title)
        possible_matches = []

        for event in db_events:
            event_date = event['start_time']
            # Игнорируем пары после 13.04.2026
            if event_date.date() > datetime.date(2026, 4, 13):
                continue
                
            event_name = event['event_name'].strip()
            
            # 2. Название без "запись" полностью совпадает с названием пары
            if event_name != clean_ktalk_title:
                continue
                
            # Если дошли сюда, значит совпало название.
            # 1. У пары ещё нет записи - это обеспечено тем, что в db_events
            # попадают только пары без записей, и мы удаляем их при привязке.
            possible_matches.append(event)

        if possible_matches:
            # Если найдено несколько пар с одинаковым названием в одну дату,
            # берем ту, что ближе по времени.
            possible_matches.sort(key=lambda x: abs((x['start_time'] - dt_msk).total_seconds()))
            best_match = possible_matches[0]
            
            print(f"✅ Успех: '{best_match['event_name']}'")
            print(f"   ⌚ Календарь: {best_match['start_time'].strftime('%d.%m %H:%M')} | Толк: {dt_msk.strftime('%d.%m %H:%M')}")
            
            update_event_record_in_db(
                event_id=best_match['event_id'],
                instance_start_ts=best_match['instance_start_ts'],
                ktalk_id=ktalk_id,
                recording_url=url,
                recording_date_str=created_date_str,
                transcription_url=transcription_url
            )
            
            # Удаляем из списка, чтобы не привязать эту же ссылку к дубликату пары
            db_events.remove(best_match) 
            success_count += 1
            log_results["success"].append(f"Запись '{ktalk_title}' ({dt_msk.strftime('%d.%m.%Y')}) -> '{best_match['event_name']}' ({best_match['start_time'].strftime('%d.%m.%Y %H:%M')})")
        else:
            print(f"⚠️ Не найдена пара в календаре для: '{ktalk_title}' ({dt_msk.strftime('%d.%m %H:%M')})")
            log_results["failed"].append(f"Не найдена пара для записи: '{ktalk_title}' ({dt_msk.strftime('%d.%m.%Y %H:%M')}) | Ссылка: {url}")

    print(f"\n🎉 Готово! Успешно привязано записей: {success_count} из {len(urls)}.")
    
    # Сохраняем лог
    log_filename = "sync_log.txt"
    log_path = os.path.join(os.path.dirname(parsed_file) if os.path.dirname(parsed_file) else '.', log_filename)
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("=== УСПЕШНО ПРИВЯЗАНО ===\n")
        if log_results["success"]:
            for line in log_results["success"]:
                f.write(line + "\n")
        else:
            f.write("Нет успешных привязок.\n")
            
        f.write("\n=== НЕ ПРИВЯЗАНО ===\n")
        if log_results["failed"]:
            for line in log_results["failed"]:
                f.write(line + "\n")
        else:
            f.write("Все записи успешно привязаны.\n")
    print(f"📝 Лог сохранен в файл: {log_path}")

if __name__ == "__main__":
    sync_records('parsed_records.txt')