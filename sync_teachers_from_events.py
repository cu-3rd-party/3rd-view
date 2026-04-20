import os
import sys
import time
import requests

# Подключаем конфигурацию и базу из твоего проекта
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config
from database import get_db_connection

def is_staff_email(email: str) -> bool:
    """Фильтрует email, оставляя только сотрудников/преподавателей"""
    e = email.lower().strip()
    if not e or "@" not in e:
        return False
        
    # Исключаем студентов
    if "@edu.centraluniversity." in e:
        return False

    # Черный список: аудитории, плазмы, доски, системные аккаунты
    stop_words = [
        "audience-",
        "room",
        "plasma",
        "board",
        "robot",
        "admin",
        "info@",
        "21-school.ru" # отсекаем железяки из Школы 21
    ]
    
    if any(stop_word in e for stop_word in stop_words):
        return False

    # Если дошло сюда, значит это скорее всего живой сотрудник/препод
    return True
def fetch_teacher_name_from_time(email: str) -> str:
    """Ищет пользователя в TiMe и возвращает Фамилия Имя"""
    username = email.split('@')[0]
    
    # URL (autocomplete)
    url = f"https://time.cu.ru/api/v4/users/autocomplete?name={username}&allow_inactive=true&is_bot=false&limit=10&boost=true&with_profiles=true"
    
    try:
        resp = requests.get(url, headers=Config.TIME_HEADERS)
        if resp.status_code == 200:
            data = resp.json()
            users = data.get("users", [])
            
            for u in users:
                # Проверяем точное совпадение по email (или хотя бы по username, если email скрыт)
                if u.get("email", "").lower() == email or u.get("username", "").lower() == username:
                    first_name = u.get("first_name", "").strip()
                    last_name = u.get("last_name", "").strip()
                    full_name = f"{last_name} {first_name}".strip()
                    
                    if full_name:
                        return full_name
                    return u.get("username", username) # Фолбек, если имя не указано
    except Exception as e:
        print(f"   [!] Ошибка API TiMe для {email}: {e}")
        
    # Если не нашли в TiMe, делаем красивый фолбек из почты (i.ivanov -> I.Ivanov)
    return username.title()

def main():
    print("🚀 Старт синхронизации преподавателей на основе событий...\n")
    conn = get_db_connection()
    
    # ЭТАП 1: Собираем email из БД
    print("[1/4] Собираем участников из событий...")
    unique_staff_emails = set()
    
    with conn.cursor() as cur:
        cur.execute("SELECT attendees_emails FROM calendar_events WHERE attendees_emails IS NOT NULL")
        rows = cur.fetchall()
        
        for row in rows:
            emails_raw = row[0]
            if not emails_raw: continue
            
            for e in emails_raw.split(','):
                email_clean = e.lower().strip()
                if is_staff_email(email_clean):
                    unique_staff_emails.add(email_clean)

    print(f"✅ Найдено уникальных email преподавателей: {len(unique_staff_emails)}\n")

    # ЭТАП 2: Парсим имена из TiMe
    print("[2/4] Запрашиваем имена из TiMe...")
    teachers_data = []
    
    for i, email in enumerate(unique_staff_emails, 1):
        print(f"   ⏳ [{i}/{len(unique_staff_emails)}] Поиск: {email}...", end="")
        full_name = fetch_teacher_name_from_time(email)
        teachers_data.append((email, full_name))
        print(f" -> {full_name}")
        time.sleep(0.3) # Пауза, чтобы не словить Rate Limit от TiMe

    # ЭТАП 3: Записываем в базу (С ОЧИСТКОЙ)
    print("\n[3/4] Очищаем старых и сохраняем новых преподавателей в PostgreSQL...")
    with conn.cursor() as cur:
        # СТИРАЕМ СТАРУЮ ТАБЛИЦУ ПРЕПОДАВАТЕЛЕЙ
        print("   🧹 Очистка таблицы teachers...")
        cur.execute("TRUNCATE TABLE teachers;")
        
        count_inserted = 0
        for email, full_name in teachers_data:
            # ON CONFLICT оставлен на всякий случай, если в множестве случайно затесались дубли
            cur.execute("""
                INSERT INTO teachers (email, full_name) 
                VALUES (%s, %s)
                ON CONFLICT (email) DO UPDATE SET full_name = EXCLUDED.full_name
            """, (email, full_name))
            count_inserted += 1
        conn.commit()
    print(f"✅ Успешно записано {count_inserted} преподавателей.\n")

    # ЭТАП 4: Пересборка имен в таблице событий
    print("[4/4] Обновляем имена преподавателей в самих событиях (calendar_events)...")
    updated_events = 0
    with conn.cursor() as cur:
        # Достаем актуальный словарь преподавателей
        cur.execute("SELECT email, full_name FROM teachers")
        teachers_dict = {row[0].strip().lower(): row[1].strip() for row in cur.fetchall()}
        
        cur.execute("SELECT event_id, instance_start_ts, attendees_emails, teacher_names FROM calendar_events")
        events = cur.fetchall()
        
        for ev in events:
            ev_id, inst_ts, attendees, current_teachers_str = ev
            if not attendees: continue
            
            emails = [e.strip().lower() for e in attendees.split(',')]
            matched_teachers = [teachers_dict[e] for e in emails if e in teachers_dict]
            new_teacher_str = ", ".join(list(set(matched_teachers))) if matched_teachers else "Не определен"
            
            if new_teacher_str != current_teachers_str:
                cur.execute("""
                    UPDATE calendar_events
                    SET teacher_names = %s
                    WHERE event_id = %s AND instance_start_ts = %s
                """, (new_teacher_str, ev_id, inst_ts))
                updated_events += 1
                
        conn.commit()
    
    print(f"✅ Обновлено событий с новыми именами: {updated_events}")
    conn.close()
    
    print("\n🎉 ГОТОВО! База преподавателей полностью перезаписана, расписание актуализировано.")

if __name__ == "__main__":
    main()