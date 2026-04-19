import os
import sys
import time
import string
import requests
import psycopg2

# Подключаем корневую директорию, чтобы видеть config.py и database.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from database import get_db_connection

def sync_teachers():
    """Собирает преподавателей из TiMe и кладет сразу в PostgreSQL"""
    print("\n[1] 🔍 Ищем канал с преподавателями...")
    channel_name = "it-support-teachers"
    
    url_ch = f"https://time.cu.ru/api/v4/teams/{Config.TIME_TEAM_ID}/channels/name/{channel_name}"
    resp = requests.get(url_ch, headers=Config.TIME_HEADERS)
    
    if resp.status_code != 200:
        print("❌ Ошибка: не удалось найти канал преподавателей. Проверь куки в .env")
        return
        
    channel_id = resp.json().get("id")
    print("✅ Канал найден! Выкачиваем участников...")
    
    users = []
    page = 0
    while True:
        url_users = f"https://time.cu.ru/api/v4/users?in_channel={channel_id}&page={page}&per_page=100"
        resp_users = requests.get(url_users, headers=Config.TIME_HEADERS)
        batch = resp_users.json()
        
        if not batch: break
            
        for u in batch:
            if u.get("is_bot"): continue
            
            email = u.get("email", "").strip().lower()
            if not email and u.get("username"):
                email = f"{u.get('username').lower()}@edu.centraluniversity.ru"
            
            first_name = u.get("first_name", "")
            last_name = u.get("last_name", "")
            full_name = f"{last_name} {first_name}".strip()
            if not full_name:
                full_name = u.get("username", "Без имени")
                
            # Исключаем студентов, если они случайно попали в чат преподов
            if email and ("@centraluniversity.ru" in email or "@edu.centraluniversity.ru" in email):
                users.append((email, full_name))
            
        if len(batch) < 100: break
        page += 1

    print(f"📥 Найдено преподавателей: {len(users)}. Записываем в базу...")
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            count = 0
            for email, full_name in users:
                cur.execute("""
                    INSERT INTO teachers (email, full_name) 
                    VALUES (%s, %s)
                    ON CONFLICT (email) DO UPDATE SET full_name = EXCLUDED.full_name
                """, (email, full_name))
                count += 1
        conn.commit()
        print(f"🎉 Успешно добавлено/обновлено {count} преподавателей в PostgreSQL!")
    finally:
        conn.close()


def discover_and_import_channels():
    """Метод брутфорса алфавита для поиска ВСЕХ каналов (курсов) и добавления их в БД"""
    print("\n[2] 🚀 Запуск сканирования ВСЕХ каналов (курсов)...")
    all_channels = {}
    queries = list(string.ascii_lowercase) + list("абвгдеёжзийклмнопрстуфхцчшщъыьэюя") + list(string.digits)
    
    for query in queries:
        url = f"https://time.cu.ru/api/v4/teams/{Config.TIME_TEAM_ID}/channels/autocomplete?name={query}&channel_types=O&channel_types=P&boost=true"
        try:
            resp = requests.get(url, headers=Config.TIME_HEADERS)
            if resp.status_code == 200:
                channels = resp.json()
                for c in channels:
                    all_channels[c["id"]] = {
                        "name": c.get("name", ""),
                        "display_name": c.get("display_name", "")
                    }
                print(f"Поиск '{query}': найдено {len(channels)}. Уникальных: {len(all_channels)}")
            elif resp.status_code == 429:
                print(f"Rate limit на '{query}'! Ждем 5 сек...")
                time.sleep(5)
        except Exception as e:
            print(f"Ошибка '{query}': {e}")
        time.sleep(0.5)

    print(f"\n📥 Сбор завершен. Всего уникальных каналов: {len(all_channels)}. Записываем в базу...")
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            count = 0
            for ch_id, ch_data in all_channels.items():
                name = ch_data["name"]
                display_name = ch_data["display_name"]
                # Формируем прямую ссылку на канал
                link = f"https://time.cu.ru/messenger/teams/{Config.TIME_TEAM_ID}/channels/{name}"
                
                # Пытаемся грубо определить трек и год из названия
                year = "2024" if "2024" in name else ("2025" if "2025" in name else "")
                track = "Неизвестно"
                if "backend" in name.lower() or "бэкенд" in name.lower(): track = "Backend"
                elif "frontend" in name.lower() or "фронтенд" in name.lower(): track = "Frontend"
                elif "analytics" in name.lower() or "аналитика" in name.lower(): track = "Analytics"

                # Проверяем, есть ли уже такой курс по ссылке
                cur.execute("SELECT id FROM courses WHERE link = %s", (link,))
                if not cur.fetchone():
                    cur.execute("""
                        INSERT INTO courses (track, course_year, level, name, search_query, link)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (track, year, "Бакалавриат", display_name, display_name, link))
                    count += 1
        conn.commit()
        print(f"🎉 Успешно добавлено {count} НОВЫХ курсов в базу!")
        print("👉 Теперь вы можете зайти в админ-панель сайта и отфильтровать/настроить их.")
    finally:
        conn.close()


def main():
    while True:
        print("\n" + "="*50)
        print("🤖 МЕНЕДЖЕР ПАРСИНГА TiMe -> PostgreSQL")
        print("="*50)
        print("1. Синхронизировать преподавателей")
        print("2. Найти все каналы-предметы и добавить в список курсов")
        print("0. Выход")
        
        choice = input("Выберите действие (0-2): ").strip()
        
        if choice == "1":
            sync_teachers()
        elif choice == "2":
            discover_and_import_channels()
        elif choice == "0":
            break
        else:
            print("❌ Неизвестная команда.")

if __name__ == "__main__":
    main()