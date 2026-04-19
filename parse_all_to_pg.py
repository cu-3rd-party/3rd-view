import csv
import time
import requests
import os
from datetime import date, timedelta
from dotenv import load_dotenv

# Твои локальные модули
from yandex_api import YandexCalendarAPI
from database import get_db_connection

# Загружаем переменные из .env
load_dotenv()

# Настройки
CSV_PATH = "new_courses.csv"
COOKIE_PATH = "cookie.txt"
DAYS_TO_PARSE = 14

TIME_TEAM_ID = os.getenv("TIME_TEAM_ID")
TIME_COOKIE = os.getenv("TIME_COOKIE")
TIME_CSRF = os.getenv("TIME_CSRF")

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "cookie": TIME_COOKIE,
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    "x-csrf-token": TIME_CSRF,
    "x-requested-with": "XMLHttpRequest"
}

def clean_database(conn):
    """Полностью очищает таблицы от старого мусора"""
    print("🧹 Очистка старой базы данных...")
    cur = conn.cursor()
    # TRUNCATE работает быстрее DELETE и обнуляет счетчики ID
    cur.execute("TRUNCATE TABLE courses, students, student_calendar_link, calendar_events RESTART IDENTITY CASCADE;")
    conn.commit()
    cur.close()
    print("✅ База девственно чиста!")

def get_channel_id(channel_name):
    url = f"https://time.cu.ru/api/v4/teams/{TIME_TEAM_ID}/channels/name/{channel_name}"
    try:
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code == 200:
            return resp.json().get("id")
    except Exception as e:
        print(f"Ошибка получения ID для {channel_name}: {e}")
    return None

def fetch_users_in_channel(channel_id):
    users = []
    page = 0
    while True:
        url = f"https://time.cu.ru/api/v4/users?in_channel={channel_id}&page={page}&per_page=100"
        try:
            resp = requests.get(url, headers=HEADERS)
            if resp.status_code == 200:
                batch = resp.json()
                users.extend(batch)
                if len(batch) < 100:
                    break
                page += 1
            else:
                break
        except Exception as e:
            print(f"Ошибка при скачивании пользователей: {e}")
            break
        time.sleep(1) # Не спамим API
    return users

def main():
    conn = get_db_connection()
    
    # 1. ЧИСТИМ БАЗУ
    clean_database(conn)
    cur = conn.cursor()

    # 2. ПАРСИМ КУРСЫ ИЗ CSV
    print("\n📚 ЭТАП 1: Загрузка курсов из CSV...")
    courses_data = []
    with open(CSV_PATH, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=",")
        for row in reader:
            link = row.get("Ссылка на канал в TiMe", "").strip()
            if not link: continue
            
            channel_name = link.split("/")[-1]
            courses_data.append({
                "track": row.get("Направление", ""),
                "course_year": row.get("Курс", ""),
                "level": row.get("Уровень", ""),
                "name": row.get("Название курса", ""),
                "link": link,
                "search_query": channel_name
            })
            
            cur.execute("""
                INSERT INTO courses (track, course_year, level, name, search_query, link)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (row.get("Направление", ""), row.get("Курс", ""), row.get("Уровень", ""), 
                  row.get("Название курса", ""), channel_name, link))
    conn.commit()
    print(f"✅ В базу добавлено {len(courses_data)} курсов.")

    # 3. ДОСТАЕМ СТУДЕНТОВ ИЗ TiMe
    print("\n👥 ЭТАП 2: Сбор студентов из TiMe...")
    all_unique_students = set()
    
    for i, course in enumerate(courses_data, 1):
        c_name = course["name"]
        ch_name = course["search_query"]
        print(f"   [{i}/{len(courses_data)}] Ищу канал: {ch_name} ({c_name})")
        
        ch_id = get_channel_id(ch_name)
        time.sleep(1)
        if not ch_id:
            print("      ❌ Канал не найден (или нет доступа)")
            continue
            
        users = fetch_users_in_channel(ch_id)
        found_students = 0
        for u in users:
            if u.get("is_bot"): continue
            email = u.get("email", "").strip().lower()
            username = u.get("username", "").strip().lower()
            
            if not email and username:
                email = f"{username}@edu.centraluniversity.ru"
                
            if email.endswith("@edu.centraluniversity.ru"):
                all_unique_students.add(email)
                found_students += 1
                
        print(f"      ✅ Студентов: {found_students}")

    print(f"\n✅ Всего найдено уникальных студентов по всем курсам: {len(all_unique_students)}")
    
    # Записываем студентов в БД
    for email in all_unique_students:
        cur.execute("INSERT INTO students (email) VALUES (%s) ON CONFLICT (email) DO NOTHING", (email,))
    conn.commit()

    # 4. ПАРСИМ ЯНДЕКС КАЛЕНДАРЬ
    print("\n📅 ЭТАП 3: Парсинг расписания Яндекса...")
    api = YandexCalendarAPI(cookie_path=COOKIE_PATH)
    start_date = date.today()
    end_date = start_date + timedelta(days=DAYS_TO_PARSE)
    
    unique_events_dict = {} 
    student_events_map = {} 
    
    # 4.1 Сетка расписания
    students_list = list(all_unique_students)
    for i, email in enumerate(students_list, 1):
        print(f"   [{i}/{len(students_list)}] Календарь: {email}")
        events = api.get_detailed_events_for_range(email, start_date, end_date)
        student_events_map[email] = []
        
        for ev in events:
            event_id = str(ev.get('event_id'))
            instance_ts = str(ev.get('instance_start_ts'))
            if not event_id or event_id == "None": continue 
            
            ev_key = (event_id, instance_ts)
            if ev_key not in unique_events_dict:
                unique_events_dict[ev_key] = ev
            student_events_map[email].append(ev_key)
            
        time.sleep(0.5)

    # 4.2 Детали пар (состав, ссылки)
    print(f"\n🔍 ЭТАП 4: Детали для {len(unique_events_dict)} уникальных пар...")
    for i, (ev_key, ev_data) in enumerate(unique_events_dict.items(), 1):
        if i % 20 == 0 or i == len(unique_events_dict):
            print(f"   -> Обработано: {i} / {len(unique_events_dict)}")
            
        link_desc, attendee_emails = api.get_event_details_by_id(ev_key[0], ev_key[1])
        ev_data['link_description'] = link_desc
        ev_data['attendees_emails'] = ",".join(attendee_emails)
        time.sleep(0.3)

    # 5. СОХРАНЯЕМ В БАЗУ
    print("\n💾 ЭТАП 5: Сохранение расписания в PostgreSQL...")
    for ev_key, ev in unique_events_dict.items():
        cur.execute("""
            INSERT INTO calendar_events 
            (event_id, instance_start_ts, start_time, end_time, event_name, total_attendees, link_description, attendees_emails, teacher_names)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (event_id, instance_start_ts) DO UPDATE SET
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                event_name = EXCLUDED.event_name,
                total_attendees = EXCLUDED.total_attendees,
                link_description = EXCLUDED.link_description,
                attendees_emails = EXCLUDED.attendees_emails;
        """, (
            str(ev_key[0]), str(ev_key[1]), ev['start'].isoformat(), ev['end'].isoformat(), 
            ev['name'], ev.get('total_attendees', 0), ev['link_description'], 
            ev['attendees_emails'], "Не определен"
        ))

    for email, event_keys in student_events_map.items():
        for ev_key in event_keys:
            cur.execute("""
                INSERT INTO student_calendar_link (student_email, event_id, instance_start_ts)
                VALUES (%s, %s, %s) 
                ON CONFLICT (student_email, event_id, instance_start_ts) DO NOTHING;
            """, (email, str(ev_key[0]), str(ev_key[1])))

    conn.commit()
    cur.close()
    conn.close()
    
    print("\n🎉 ГОТОВО! База очищена, курсы загружены, студенты спарсены, расписание обновлено.")

if __name__ == "__main__":
    main()