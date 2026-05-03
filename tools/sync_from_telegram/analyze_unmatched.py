import os
import datetime
from zoneinfo import ZoneInfo
import re
import requests
import psycopg2

DB_CONFIG = {
    'dbname': 'cu_view_db',
    'user': 'cu_view_user',
    'password': 'SRY_NO_DOCKER_PASSWORD',
    'host': '127.0.0.1',
    'port': '5432'
}

def get_attached_urls():
    query = "SELECT recording_url FROM event_recordings WHERE recording_url IS NOT NULL;"
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return set(row[0] for row in cur.fetchall())
    except Exception as e:
        print(f"Ошибка получения ссылок из БД: {e}")
        return set()

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
        except Exception:
            pass
            
    return events

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
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json", "X-Platform": "web"}
    if token: headers["Authorization"] = token

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 401: return None
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None

def clean_record_title(title):
    t = re.sub(r'(?i)запись', '', title).strip()
    return t

def analyze(parsed_file):
    print("🚀 Запускаем анализ непривязанных записей...")
    
    try:
        with open(parsed_file, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"❌ Файл {parsed_file} не найден!")
        return

    attached_urls = get_attached_urls()
    db_events = get_events_without_records_from_db()
    msk_zone = ZoneInfo("Europe/Moscow")

    unmatched_urls = [u for u in urls if u not in attached_urls]
    print(f"🔎 Найдено {len(unmatched_urls)} непривязанных ссылок из {len(urls)}.")

    report_lines = []
    report_lines.append("=== АНАЛИЗ НЕПРИВЯЗАННЫХ ЗАПИСЕЙ ===\n")

    for url in unmatched_urls:
        ktalk_id = url.split('/')[-1]
        record_data = get_ktalk_record_info(ktalk_id)
        if not record_data:
            report_lines.append(f"❌ Не удалось получить данные по API для: {url}\n")
            continue
            
        ktalk_title = record_data.get('title', '')
        created_date_str = record_data.get('createdDate')
        
        try:
            clean_date_str = re.sub(r'\.\d+', '', created_date_str).replace("Z", "+00:00")
            dt_utc = datetime.datetime.fromisoformat(clean_date_str)
            dt_msk = dt_utc.astimezone(msk_zone)
        except Exception:
            continue

        clean_title = clean_record_title(ktalk_title)
        
        report_lines.append("--------------------------------------------------")
        report_lines.append(f"🔴 ЗАПИСЬ: {ktalk_title}")
        report_lines.append(f"   Очищенное название: '{clean_title}'")
        report_lines.append(f"   Дата записи: {dt_msk.strftime('%d.%m.%Y %H:%M')}")
        report_lines.append(f"   Ссылка: {url}")
        
        # Ищем возможные пары в этот же день
        same_day_events = [e for e in db_events if e['start_time'].date() == dt_msk.date()]
        
        if not same_day_events:
            report_lines.append("   ⚠️ В этот день в календаре вообще нет пар без записей!")
        else:
            report_lines.append("   📝 Пары без записей в этот день (возможные кандидаты):")
            # Сортируем по времени
            same_day_events.sort(key=lambda x: x['start_time'])
            for e in same_day_events:
                diff_mins = abs((e['start_time'] - dt_msk).total_seconds()) / 60.0
                report_lines.append(f"      - [{e['start_time'].strftime('%H:%M')}] {e['event_name']}")
                report_lines.append(f"        (Разница с записью: {diff_mins:.0f} мин)")

        report_lines.append("")

    report_filename = "unmatched_analysis.txt"
    log_path = os.path.join(os.path.dirname(parsed_file) if os.path.dirname(parsed_file) else '.', report_filename)
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
        
    print(f"✅ Анализ завершен! Подробный отчет сохранен в {log_path}")

if __name__ == "__main__":
    analyze('parsed_records.txt')
