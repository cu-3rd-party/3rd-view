import os
import time
import json
import asyncio
import requests
import re
import datetime
import logging
from datetime import date, timedelta, timezone

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

# --- Импорты из наших новых модулей ---
from config import Config
from database import get_db_connection
from schemas import *
from auth import (
    get_password_hash, 
    verify_password, 
    create_access_token,
    send_verification_email, 
    verify_user_or_admin, 
    verify_admin, 
    get_current_user
)
from integrations.yandex_api import YandexCalendarAPI
from integrations.ktalk_api import KTalkAPI

import random
import string

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

app = FastAPI(title="University Calendar API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_DOMAINS = ["@edu.centraluniversity.ru", "@centraluniversity.ru"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

# ==========================================
# РОУТЫ И API: АВТОРИЗАЦИЯ
# ==========================================

@app.post("/api/auth/register")
async def register_user(data: RegisterRequest):
    email = data.email.lower().strip()
    if not any(email.endswith(domain) for domain in ALLOWED_DOMAINS):
        raise HTTPException(status_code=400, detail="Разрешены только корпоративные почты")

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (email,))
            user = cur.fetchone()
            
            if user and user['is_active']:
                raise HTTPException(status_code=400, detail="Email уже зарегистрирован")
                
            code = "".join(random.choices(string.digits, k=6))
            hashed_pwd = get_password_hash(data.password)

            if user and not user['is_active']:
                cur.execute("UPDATE users SET password_hash = %s WHERE email = %s", (hashed_pwd, email))
            else:
                cur.execute("INSERT INTO users (email, password_hash) VALUES (%s, %s)", (email, hashed_pwd))
            
            cur.execute("""
                INSERT INTO email_verifications (email, code, created_at) 
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (email) DO UPDATE SET code = EXCLUDED.code, created_at = CURRENT_TIMESTAMP
            """, (email, code))
            
            try:
                send_verification_email(email, code)
            except Exception as e:
                conn.rollback()
                raise HTTPException(status_code=500, detail=f"Ошибка отправки почты: {str(e)}")
                
        conn.commit()
        return {"status": "ok", "message": "Код отправлен на почту"}
    finally:
        conn.close()

@app.post("/api/auth/verify")
async def verify_user(data: VerifyRequest):
    email = data.email.lower().strip()
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur: 
            cur.execute("SELECT code FROM email_verifications WHERE email = %s", (email,))
            row = cur.fetchone()
            
            if not row or row['code'] != data.code.strip():
                raise HTTPException(status_code=400, detail="Неверный код")
                
            cur.execute("UPDATE users SET is_active = TRUE WHERE email = %s", (email,))
            cur.execute("DELETE FROM email_verifications WHERE email = %s", (email,))
        conn.commit()
        return {"status": "ok"}
    finally:
        conn.close()

@app.post("/api/auth/login")
async def login_user(data: LoginRequest):
    email = data.email.lower().strip()
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (email,))
            user = cur.fetchone()
            
            if not user or not verify_password(data.password, user['password_hash']):
                raise HTTPException(status_code=401, detail="Неверный логин или пароль")
            if not user['is_active']:
                raise HTTPException(status_code=403, detail="Почта не подтверждена")
                
            token = create_access_token({"sub": email})
            return {"access_token": token}
    finally:
        conn.close()

# ==========================================
# РОУТЫ: HTML СТРАНИЦЫ
# ==========================================

@app.get("/", response_class=HTMLResponse)
async def read_index():
    file_path = os.path.join(TEMPLATES_DIR, "index.html")
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/admin", response_class=HTMLResponse)
async def read_admin(admin: str = Depends(verify_admin)):
    file_path = os.path.join(TEMPLATES_DIR, "admin.html")
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

# ==========================================
# РОУТЫ: КУРСЫ И ПАРСИНГ ПРЕДМЕТОВ (TiMe)
# ==========================================

@app.get("/api/courses")
async def get_courses(auth: dict = Depends(verify_user_or_admin)):
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, track, course_year, level, name, search_query, link 
            FROM courses 
            ORDER BY track, course_year, name
        """)
        courses = cur.fetchall()
    conn.close()
    return [dict(row) for row in courses]

@app.post("/api/courses/{course_id}")
async def update_course(course_id: int, data: CourseUpdateModel, admin: str = Depends(verify_admin)):
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("UPDATE courses SET search_query = %s, link = %s WHERE id = %s", 
                     (data.search_query.strip(), data.link.strip(), course_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

async def scan_generator(course_id: int):
    def emit(msg_type, data):
        return json.dumps({"type": msg_type, "data": data}) + "\n"
        
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT link FROM courses WHERE id = %s", (course_id,))
            course = cur.fetchone()
        conn.close()
        
        if not course or not course["link"]:
            yield emit("error", "У курса нет ссылки на TiMe")
            return

        link = course["link"].strip()
        channel_name = link.split("/")[-1]
        
        yield emit("progress", f"🔍 Поиск канала <b>{channel_name}</b>...")
        await asyncio.sleep(0.2)

        url_ch = f"https://time.cu.ru/api/v4/teams/{Config.TIME_TEAM_ID}/channels/name/{channel_name}"
        resp = requests.get(url_ch, headers=Config.TIME_HEADERS)
        if resp.status_code != 200:
            yield emit("error", "Канал не найден. Проверь куки/ссылку.")
            return
            
        channel_id = resp.json().get("id")
        users = requests.get(f"https://time.cu.ru/api/v4/users?in_channel={channel_id}&page=0&per_page=100", headers=Config.TIME_HEADERS).json()
        
        valid_emails = [u.get("email", "").strip().lower() or f"{u.get('username', '').lower()}@edu.centraluniversity.ru" for u in users if not u.get("is_bot")]
        valid_emails = list(set(e for e in valid_emails if e.endswith("@edu.centraluniversity.ru")))
        target_students = valid_emails[:50]
        
        if not target_students:
            yield emit("error", "В канале нет студентов")
            return

        yield emit("progress", f"✅ Найдено {len(target_students)} студентов. Сканируем их календари...")
        api = YandexCalendarAPI(cookie_path="cookie.txt")
        start_date = datetime.date.today()
        end_date = start_date + timedelta(days=14) 
        found_names = set()
        
        for i, email in enumerate(target_students, 1):
            yield emit("progress", f"👀 Читаем расписание {i}/{len(target_students)}: <small>{email}</small>")
            await asyncio.sleep(0.1)
            events = api.get_detailed_events_for_range(email, start_date, end_date)
            for ev in events:
                found_names.add(ev['name'])
        
        yield emit("progress", "🎉 Сканирование завершено. Выберите нужное название ниже.")
        yield emit("done", {"names": list(found_names)})
    except Exception as e:
        yield emit("error", f"Ошибка сканирования: {str(e)}")

@app.post("/api/courses/{course_id}/scan")
async def scan_course_endpoint(course_id: int, admin: str = Depends(verify_admin)):
    return StreamingResponse(scan_generator(course_id), media_type="text/plain")

async def extract_generator(course_id: int, search_query: str):
    def emit(msg_type, data):
        return json.dumps({"type": msg_type, "data": data}) + "\n"
        
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT name, link FROM courses WHERE id = %s", (course_id,))
            course = cur.fetchone()
        
        link = course["link"].strip()
        channel_name = link.split("/")[-1]
        
        yield emit("progress", f"🚀 Ищем целевые пары по запросу: <b>{search_query}</b>...")
        await asyncio.sleep(0.2)
        
        url_ch = f"https://time.cu.ru/api/v4/teams/{Config.TIME_TEAM_ID}/channels/name/{channel_name}"
        channel_id = requests.get(url_ch, headers=Config.TIME_HEADERS).json().get("id")
        users = requests.get(f"https://time.cu.ru/api/v4/users?in_channel={channel_id}&page=0&per_page=100", headers=Config.TIME_HEADERS).json()
        
        valid_emails = [u.get("email", "").strip().lower() or f"{u.get('username', '').lower()}@edu.centraluniversity.ru" for u in users if not u.get("is_bot")]
        target_students = list(set(e for e in valid_emails if e.endswith("@edu.centraluniversity.ru")))[:7]
        
        api = YandexCalendarAPI(cookie_path="cookie.txt")
        start_date = datetime.date.today()
        end_date = start_date + timedelta(days=14)
        
        unique_events_dict = {}
        student_links = []
        cur = conn.cursor()
        
        for email in target_students:
            cur.execute("INSERT INTO students (email) VALUES (%s) ON CONFLICT (email) DO NOTHING", (email,))
            events = api.get_detailed_events_for_range(email, start_date, end_date)
            for ev in events:
                event_id = ev.get('event_id')
                if not event_id: continue
                if search_query.lower() in ev['name'].lower():
                    ev_key = (event_id, ev.get('instance_start_ts'))
                    unique_events_dict[ev_key] = ev
                    student_links.append((email, ev_key[0], ev_key[1]))

        total_target = len(unique_events_dict)
        if total_target == 0:
            yield emit("error", "По такому запросу пар не найдено.")
            conn.close()
            return
            
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as t_cur:
                t_cur.execute("SELECT email, full_name FROM teachers")
                teachers_raw = t_cur.fetchall()
            teachers_dict = {row['email'].lower(): row['full_name'] for row in teachers_raw}
        except Exception:
            teachers_dict = {}

        yield emit("progress", f"🎯 Найдено пар по предмету: {total_target}. Достаем ссылки...")

        for i, (ev_key, ev_data) in enumerate(unique_events_dict.items(), 1):
            yield emit("progress", f"🔗 Парсинг деталей: {i} / {total_target}...")
            await asyncio.sleep(0.3)
            
            link_desc, attendees_emails = api.get_event_details_by_id(ev_key[0], ev_key[1])
            start_str = ev_data['start'].isoformat()
            end_str = ev_data['end'].isoformat()
            attendees_str = ",".join(attendees_emails)
            
            matched_teachers = [teachers_dict[e] for e in attendees_emails if e in teachers_dict]
            teacher_names_str = ", ".join(list(set(matched_teachers))) if matched_teachers else "Не определен"
            
            cur.execute("""
                INSERT INTO calendar_events 
                (event_id, instance_start_ts, start_time, end_time, event_name, total_attendees, link_description, attendees_emails, teacher_names)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                ON CONFLICT (event_id, instance_start_ts) DO UPDATE SET
                link_description = excluded.link_description,
                attendees_emails = excluded.attendees_emails,
                teacher_names = excluded.teacher_names
            """, (ev_key[0], ev_key[1], start_str, end_str, ev_data['name'], ev_data['total_attendees'], link_desc, attendees_str, teacher_names_str))

        for link_tuple in student_links:
            cur.execute("""
                INSERT INTO student_calendar_link (student_email, event_id, instance_start_ts) 
                VALUES (%s, %s, %s) ON CONFLICT (student_email, event_id, instance_start_ts) DO NOTHING
            """, link_tuple)

        conn.commit()
        conn.close()
        yield emit("progress", "✅ Пары и ссылки успешно сохранены!")
        yield emit("done", {"count": total_target})
    except Exception as e:
        yield emit("error", f"Ошибка выгрузки: {str(e)}")

@app.post("/api/courses/{course_id}/extract")
async def extract_course_endpoint(course_id: int, search_query: str = Query(...), admin: str = Depends(verify_admin)):
    return StreamingResponse(extract_generator(course_id, search_query), media_type="text/plain")

# ==========================================
# РОУТЫ: ВЫДАЧА И ПОИСК СОБЫТИЙ
# ==========================================

@app.post("/api/events")
async def get_events(data: EventsRequest, auth: dict = Depends(verify_user_or_admin)):
    if not data.filters:
        return []

    conn = get_db_connection()
    query = """
        SELECT start_time, end_time, event_name as title, 
               total_attendees, link_description, teacher_names, attendees_emails, event_id, instance_start_ts
        FROM calendar_events
        WHERE event_name != 'Событие скрыто' AND event_name != 'Занят'
    """
    params = []
    
    course_clauses = []
    for f in data.filters:
        if not f.teachers: continue
        t_clauses = " OR ".join(["teacher_names LIKE %s" for _ in f.teachers])
        course_clauses.append(f"(event_name LIKE %s AND ({t_clauses}))")
        params.append(f"%{f.query}%")
        params.extend([f"%{t}%" for t in f.teachers])
        
    if not course_clauses:
        conn.close()
        return []
        
    query += f" AND ({' OR '.join(course_clauses)})"
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, tuple(params))
        events_raw = cur.fetchall()
    
    if not events_raw:
        conn.close()
        return []

    event_ids = list(set([str(row['event_id']) for row in events_raw]))
    placeholders = ','.join(['%s'] * len(event_ids))
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT yandex_event_id, yandex_instance_start_ts, recording_url, recording_date 
            FROM event_recordings 
            WHERE yandex_event_id IN ({placeholders})
        """, tuple(event_ids))
        recs_raw = cur.fetchall()
        
    conn.close()

    recs_by_event = {}
    msk_tz = datetime.timezone(datetime.timedelta(hours=3))
    
    for r in recs_raw:
        eid_str = str(r['yandex_event_id'])
        date_key = r['recording_date']
        
        if not date_key and r['yandex_instance_start_ts'] and r['yandex_instance_start_ts'].endswith('Z'):
            try:
                ts_dt = datetime.datetime.fromisoformat(r['yandex_instance_start_ts'].replace('Z', '+00:00'))
                date_key = ts_dt.astimezone(msk_tz).strftime('%Y-%m-%d')
            except: pass
                
        if not date_key: continue
            
        if eid_str not in recs_by_event: 
            recs_by_event[eid_str] = {}
            
        if r['recording_url']:
            recs_by_event[eid_str][date_key] = {"url": r['recording_url']}
    
    absolute_events = []
    colors = ['#3788d8', '#28a745', '#dc3545', '#fd7e14', '#6f42c1', '#20c997']
    
    for row in events_raw:
        try:
            st = datetime.datetime.fromisoformat(row['start_time'].replace('Z', '+00:00'))
            et = datetime.datetime.fromisoformat(row['end_time'].replace('Z', '+00:00'))
        except Exception: continue
        
        title = row['title']
        teacher_names = row['teacher_names']
        eid_str = str(row['event_id'])
        inst_ts = str(row.get('instance_start_ts', ''))
        
        color_idx = hash(title.split(',')[0]) % len(colors)
        color = colors[color_idx]
        
        date_str = st.astimezone(msk_tz).strftime('%Y-%m-%d')
        
        event_recs = recs_by_event.get(eid_str, {})
        day_recording = event_recs.get(date_str)
        
        recordings_dict = {}
        if day_recording:
            recordings_dict[date_str] = day_recording

        absolute_events.append({
            "id": f"{eid_str}_{inst_ts}",
            "template_event_id": eid_str,
            "title": title,
            "start": st.isoformat(),
            "end": et.isoformat(),
            "backgroundColor": color,
            "borderColor": color,
            "total_attendees": row['total_attendees'],
            "link_description": row['link_description'],
            "teacher_names": teacher_names,
            "attendees_emails": row['attendees_emails'],
            "recordings": recordings_dict 
        })
        
    return absolute_events

# ==========================================
# РОУТЫ: ТОТАЛЬНАЯ СИНХРОНИЗАЦИЯ (АДМИН)
# ==========================================

async def total_sync_generator():
    def emit(msg_type, data):
        return json.dumps({"type": msg_type, "data": data}) + "\n"
        
    try:
        DAYS_AHEAD = 30
        yield emit("progress", f"🚀 Запуск ТОТАЛЬНОЙ синхронизации (на {DAYS_AHEAD} дней)...")
        api = YandexCalendarAPI(cookie_path="cookie.txt")
        start_date = datetime.date.today()
        end_date = start_date + timedelta(days=DAYS_AHEAD)
        
        conn = get_db_connection()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT email, full_name FROM teachers")
            teachers_dict = {row['email'].lower(): row['full_name'] for row in cur.fetchall()}
            
            cur.execute("SELECT link FROM courses WHERE link IS NOT NULL AND link != ''")
            courses = cur.fetchall()

        all_unique_students = set()
        yield emit("progress", "🕵️ Сбор базы студентов из TiMe...")
        await asyncio.sleep(0.1)

        for course in courses:
            link = course['link'].strip()
            if not link: continue
            channel_name = link.split("/")[-1]
            
            url_ch = f"https://time.cu.ru/api/v4/teams/{Config.TIME_TEAM_ID}/channels/name/{channel_name}"
            resp = requests.get(url_ch, headers=Config.TIME_HEADERS)
            if resp.status_code == 200:
                channel_id = resp.json().get("id")
                page = 0
                while True:
                    chunk = requests.get(f"https://time.cu.ru/api/v4/users?in_channel={channel_id}&page={page}&per_page=100", headers=Config.TIME_HEADERS).json()
                    if not chunk: break
                    valid_emails = [u.get("email", "").strip().lower() or f"{u.get('username', '').lower()}@edu.centraluniversity.ru" for u in chunk if not u.get("is_bot")]
                    all_unique_students.update([e for e in valid_emails if e.endswith("@edu.centraluniversity.ru")])
                    page += 1
                    await asyncio.sleep(0.1)
        
        students_list = list(all_unique_students)
        yield emit("progress", f"👥 Собрано уникальных студентов: {len(students_list)}")
        
        unique_events_dict = {}
        student_links = []
        
        cur = conn.cursor()
        for i, email in enumerate(students_list, 1):
            if i % 5 == 0:
                yield emit("progress", f"📅 Проверено календарей: {i} / {len(students_list)}")
            
            cur.execute("INSERT INTO students (email) VALUES (%s) ON CONFLICT (email) DO NOTHING", (email,))
            
            events = api.get_detailed_events_for_range(email, start_date, end_date)
            for ev in events:
                if not ev.get('event_id'): continue
                if ev['name'] in ["Занят", "Событие скрыто"]: continue
                
                ev_key = (ev['event_id'], ev.get('instance_start_ts'))
                if ev_key not in unique_events_dict:
                    unique_events_dict[ev_key] = ev
                student_links.append((email, ev_key[0], ev_key[1]))
            await asyncio.sleep(0.2)
            
        total_events = len(unique_events_dict)
        yield emit("progress", f"🎯 Найдено уникальных пар (за 30 дней): {total_events}. Выгружаем ссылки...")

        for j, (ev_key, ev_data) in enumerate(unique_events_dict.items(), 1):
            if j % 10 == 0:
                yield emit("progress", f"🔗 Скачано деталей: {j} / {total_events}")
                
            link_desc, attendees_emails = api.get_event_details_by_id(ev_key[0], ev_key[1])
            start_str = ev_data['start'].isoformat()
            end_str = ev_data['end'].isoformat()
            attendees_str = ",".join(attendees_emails)
            
            matched_teachers = [teachers_dict[e] for e in attendees_emails if e in teachers_dict]
            teacher_names_str = ", ".join(list(set(matched_teachers))) if matched_teachers else "Не определен"
            
            cur.execute("""
                INSERT INTO calendar_events 
                (event_id, instance_start_ts, start_time, end_time, event_name, total_attendees, link_description, attendees_emails, teacher_names)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                ON CONFLICT (event_id, instance_start_ts) DO UPDATE SET
                link_description = excluded.link_description,
                attendees_emails = excluded.attendees_emails,
                teacher_names = excluded.teacher_names,
                total_attendees = excluded.total_attendees
            """, (ev_key[0], ev_key[1], start_str, end_str, ev_data['name'], ev_data['total_attendees'], link_desc, attendees_str, teacher_names_str))
            await asyncio.sleep(0.2)

        yield emit("progress", "🧠 Привязываем студентов к парам...")
        for link_tuple in student_links:
            cur.execute("""
                INSERT INTO student_calendar_link (student_email, event_id, instance_start_ts) 
                VALUES (%s, %s, %s) ON CONFLICT (student_email, event_id, instance_start_ts) DO NOTHING
            """, link_tuple)

        conn.commit()
        conn.close()
        yield emit("done", {"message": "🎉 ТОТАЛЬНАЯ синхронизация успешно завершена!"})
    except Exception as e:
        yield emit("error", f"Ошибка: {str(e)}")

@app.post("/api/admin/total_sync")
async def run_total_sync_endpoint(admin: str = Depends(verify_admin)):
    return StreamingResponse(total_sync_generator(), media_type="text/plain")

# ==========================================
# РОУТЫ: ПРЕПОДАВАТЕЛИ
# ==========================================

@app.get("/api/teachers")
async def get_teachers(queries: Optional[List[str]] = Query(None), auth: dict = Depends(verify_user_or_admin)):
    if not queries: return {}
    conn = get_db_connection()
    result = {}
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for q in queries:
            if not q.strip() or q == 'null': continue
            cur.execute("SELECT DISTINCT teacher_names FROM calendar_events WHERE event_name LIKE %s", (f"%{q}%",))
            rows = cur.fetchall()
            
            t_set = set()
            for row in rows:
                if row['teacher_names']:
                    for t in row['teacher_names'].split(','):
                        t_stripped = t.strip()
                        if t_stripped: t_set.add(t_stripped)
            
            t_list = sorted(list(t_set))
            if 'Не определен' in t_list:
                t_list.remove('Не определен')
                t_list.append('Не определен')
                
            result[q] = t_list
        
    conn.close()
    return result

@app.post("/api/teachers")
async def add_teacher(data: NewTeacherModel, admin: str = Depends(verify_admin)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO teachers (email, full_name) VALUES (%s, %s)", 
                        (data.email.lower().strip(), data.full_name.strip()))
        conn.commit()
        return {"status": "ok"}
    except psycopg2.errors.UniqueViolation:
        return {"status": "error", "message": "Преподаватель с таким Email уже существует"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()

@app.post("/api/admin/recheck_teachers")
async def recheck_teachers(admin: str = Depends(verify_admin)):
    conn = get_db_connection()
    updated_count = 0
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT email, full_name FROM teachers WHERE email IS NOT NULL")
            teachers_raw = cur.fetchall()
            teachers_dict = {
                row['email'].strip().lower(): row['full_name'].strip() 
                for row in teachers_raw if row.get('email')
            }

            cur.execute("SELECT event_id, instance_start_ts, attendees_emails, teacher_names FROM calendar_events")
            events = cur.fetchall()

            for ev in events:
                if not ev['attendees_emails']: continue
                
                emails = [e.strip().lower() for e in ev['attendees_emails'].split(',')]
                matched_teachers = [teachers_dict[e] for e in emails if e in teachers_dict]
                new_teacher_str = ", ".join(list(set(matched_teachers))) if matched_teachers else "Не определен"

                if new_teacher_str != ev['teacher_names']:
                    cur.execute("""
                        UPDATE calendar_events
                        SET teacher_names = %s
                        WHERE event_id = %s AND instance_start_ts = %s
                    """, (new_teacher_str, ev['event_id'], ev['instance_start_ts']))
                    updated_count += 1
                    
        conn.commit()
        return {"status": "ok", "updated": updated_count}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()

# ==========================================
# РОУТЫ: ЗАПИСИ ЗАНЯТИЙ (KTALK)
# ==========================================

async def sync_recordings_generator():
    def emit(msg_type, data):
        return json.dumps({"type": msg_type, "data": data}) + "\n"
        
    try:
        yield emit("progress", "🔌 Подключение к KTalk API...")
        ktalk = KTalkAPI()
        
        conferences = ktalk.get_all_history_records(max_pages=10) 
        recordings = [conf for conf in conferences if conf.get('has_recording') and conf.get('recording_url')]
        yield emit("progress", f"📥 Загружено {len(conferences)} конференций, из них {len(recordings)} с записями.")
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT event_id, start_time, event_name FROM calendar_events")
            db_events_raw = cur.fetchall()
        
        parsed_db_events = []
        for dbe in db_events_raw:
            st = datetime.datetime.fromisoformat(dbe['start_time'].replace('Z', '+00:00'))
            parsed_db_events.append({
                'event_id': dbe['event_id'],
                'start_dt': st,
                'name': dbe['event_name'].lower()
            })

        matched_count = 0
        cur = conn.cursor()

        yield emit("progress", "🧠 Анализ совпадений (название/комната + день недели + время)...")
        await asyncio.sleep(0.5)

        msk_tz = datetime.timezone(datetime.timedelta(hours=3))

        for rec in recordings:
            raw_date = rec['start_time'].replace('Z', '')
            clean_date = raw_date.split('.')[0] + '+00:00'
            
            try:
                rec_dt = datetime.datetime.fromisoformat(clean_date).astimezone(msk_tz)
            except Exception:
                continue
                
            rec_date_str = rec_dt.strftime('%Y-%m-%d')
            rec_title = f"{rec.get('title', '')} {rec.get('room_name', '')}".lower()

            for dbe in parsed_db_events:
                if dbe['name'] in rec_title or rec_title in dbe['name']:
                    if rec_dt.weekday() == dbe['start_dt'].weekday():
                        rec_hours = rec_dt.hour + rec_dt.minute / 60.0
                        dbe_hours = dbe['start_dt'].hour + dbe['start_dt'].minute / 60.0
                        time_diff = abs(rec_hours - dbe_hours)
                        
                        if time_diff <= 2.0:
                            fake_ts = f"ktalk_{rec['recording_id']}"
                            cur.execute("""
                                INSERT INTO event_recordings 
                                (yandex_event_id, yandex_instance_start_ts, ktalk_id, recording_url, transcription_url, is_manual, recording_date)
                                VALUES (%s, %s, %s, %s, '', FALSE, %s)
                                ON CONFLICT (yandex_event_id, yandex_instance_start_ts) DO UPDATE SET
                                recording_url = excluded.recording_url
                            """, (dbe['event_id'], fake_ts, rec['recording_id'], rec['recording_url'], rec_date_str))
                            matched_count += 1
                            break

        conn.commit()
        conn.close()
        yield emit("done", {"count": matched_count})
        
    except Exception as e:
        yield emit("error", f"Ошибка: {str(e)}")

@app.post("/api/recordings/sync")
async def sync_recordings_endpoint(admin: str = Depends(verify_admin)):
    return StreamingResponse(sync_recordings_generator(), media_type="text/plain")

@app.post("/api/recordings/manual")
async def set_manual_recording(data: ManualRecordingModel, admin: str = Depends(verify_admin)):
    conn = get_db_connection()
    fake_ts = f"manual_{data.recording_date}"
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO event_recordings 
            (yandex_event_id, yandex_instance_start_ts, recording_url, is_manual, recording_date)
            VALUES (%s, %s, %s, TRUE, %s)
            ON CONFLICT (yandex_event_id, yandex_instance_start_ts) DO UPDATE SET
            recording_url = excluded.recording_url,
            is_manual = TRUE
        """, (data.yandex_event_id, fake_ts, data.recording_url.strip(), data.recording_date))
    conn.commit()
    conn.close()
    return {"status": "ok"}

# ==========================================
# РОУТЫ: ИМПОРТ И ПОИСК СТУДЕНТАМИ
# ==========================================

@app.get("/api/student/import")
async def import_student_schedule(email: str, user: str = Depends(get_current_user)):
    email = email.lower().strip()
    if not email.endswith("@edu.centraluniversity.ru"):
        return {"status": "error", "message": "Введите корпоративную почту (@edu.centraluniversity.ru)"}

    api = YandexCalendarAPI(cookie_path="cookie.txt")
    start_date = datetime.date.today() - timedelta(days=7)
    end_date = datetime.date.today() + timedelta(days=30)

    raw_events = api.get_detailed_events_for_range(email, start_date, end_date)
    if not raw_events:
        return {"status": "ok", "events": []}

    event_ids = list(set([str(e['event_id']) for e in raw_events if e.get('event_id')]))
    
    db_events_dict = {}
    recs_by_event = {}
    teachers_dict = {}
    msk_tz = datetime.timezone(datetime.timedelta(hours=3))
    colors = ['#3788d8', '#28a745', '#dc3545', '#fd7e14', '#6f42c1', '#20c997']

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT email, full_name FROM teachers")
            for row in cur.fetchall():
                teachers_dict[row['email'].lower()] = row['full_name']

            if event_ids:
                placeholders = ','.join(['%s'] * len(event_ids))
                cur.execute(f"""
                    SELECT event_id, instance_start_ts, total_attendees, link_description, teacher_names, attendees_emails
                    FROM calendar_events WHERE event_id IN ({placeholders})
                """, tuple(event_ids))
                for row in cur.fetchall():
                    db_events_dict[(str(row['event_id']), str(row['instance_start_ts']))] = row

                cur.execute(f"""
                    SELECT yandex_event_id, yandex_instance_start_ts, recording_url, recording_date 
                    FROM event_recordings WHERE yandex_event_id IN ({placeholders})
                """, tuple(event_ids))
                for r in cur.fetchall():
                    eid = str(r['yandex_event_id'])
                    date_key = r['recording_date']
                    if not date_key and r['yandex_instance_start_ts']:
                        try:
                            ts_dt = datetime.datetime.fromisoformat(r['yandex_instance_start_ts'].replace('Z', '+00:00'))
                            date_key = ts_dt.astimezone(msk_tz).strftime('%Y-%m-%d')
                        except: pass
                    if date_key and r['recording_url']:
                        if eid not in recs_by_event: recs_by_event[eid] = {}
                        recs_by_event[eid][date_key] = {"url": r['recording_url']}
    except Exception as e:
        print(f"DB Error: {e}")
        
    formatted_events = []
    cur = conn.cursor()
    
    for ev in raw_events:
        if ev['name'] in ["Занят", "Событие скрыто"]: continue
        
        eid = str(ev.get('event_id', ''))
        inst = str(ev.get('instance_start_ts', ''))
        db_data = db_events_dict.get((eid, inst))

        if not db_data or not db_data.get('attendees_emails'):
            link_desc, attendees_emails = api.get_event_details_by_id(eid, inst)
            
            attendees_str = ",".join(attendees_emails)
            matched_teachers = [teachers_dict[e] for e in attendees_emails if e in teachers_dict]
            teacher_names_str = ", ".join(list(set(matched_teachers))) if matched_teachers else "Не определен"
            
            start_str = ev['start'].isoformat()
            end_str = ev['end'].isoformat()
            
            db_data = {
                'total_attendees': ev.get('total_attendees', len(attendees_emails)),
                'link_description': link_desc,
                'teacher_names': teacher_names_str,
                'attendees_emails': attendees_str
            }
            
            try:
                cur.execute("""
                    INSERT INTO calendar_events 
                    (event_id, instance_start_ts, start_time, end_time, event_name, total_attendees, link_description, attendees_emails, teacher_names)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                    ON CONFLICT (event_id, instance_start_ts) DO UPDATE SET
                    link_description = excluded.link_description,
                    attendees_emails = excluded.attendees_emails,
                    teacher_names = excluded.teacher_names,
                    total_attendees = excluded.total_attendees
                """, (eid, inst, start_str, end_str, ev['name'], db_data['total_attendees'], link_desc, attendees_str, teacher_names_str))
                conn.commit()
            except Exception as e:
                conn.rollback()
                print("DB Save Error:", e)

        color_idx = hash(ev['name'].split(',')[0]) % len(colors)
        color = colors[color_idx]
        date_str = ev['start'].astimezone(msk_tz).strftime('%Y-%m-%d')
        
        recordings_dict = {}
        if eid in recs_by_event and date_str in recs_by_event[eid]:
            recordings_dict[date_str] = recs_by_event[eid][date_str]

        formatted_events.append({
            "id": f"{eid}_{inst}",
            "template_event_id": eid,
            "title": ev['name'],
            "start": ev['start'].isoformat(),
            "end": ev['end'].isoformat(),
            "backgroundColor": color,
            "borderColor": color,
            "total_attendees": db_data.get('total_attendees', 0),
            "link_description": db_data.get('link_description', ''),
            "teacher_names": db_data.get('teacher_names', 'Не определен'),
            "attendees_emails": db_data.get('attendees_emails', ''),
            "recordings": recordings_dict
        })

    cur.close()
    conn.close()

    return {"status": "ok", "events": formatted_events}

@app.get("/api/events/search_specific")
async def search_specific_events(query: str, teacher: str, user: str = Depends(get_current_user)):
    conn = get_db_connection()
    msk_tz = datetime.timezone(datetime.timedelta(hours=3))
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        search_start = (datetime.datetime.now() - datetime.timedelta(days=2)).isoformat()
        cur.execute("""
            SELECT event_id, instance_start_ts, start_time, end_time, event_name, 
                   total_attendees, link_description, teacher_names, attendees_emails
            FROM calendar_events
            WHERE event_name LIKE %s AND teacher_names LIKE %s AND start_time > %s
            ORDER BY start_time ASC
        """, (f"%{query}%", f"%{teacher}%", search_start))
        db_events = cur.fetchall()

        event_ids = list(set([str(row['event_id']) for row in db_events]))
        recs_by_event = {}
        
        if event_ids:
            placeholders = ','.join(['%s'] * len(event_ids))
            cur.execute(f"""
                SELECT yandex_event_id, yandex_instance_start_ts, recording_url, recording_date 
                FROM event_recordings 
                WHERE yandex_event_id IN ({placeholders})
            """, tuple(event_ids))
            recs_raw = cur.fetchall()
            
            for r in recs_raw:
                eid_str = str(r['yandex_event_id'])
                date_key = r['recording_date']
                
                if not date_key and r['yandex_instance_start_ts'] and r['yandex_instance_start_ts'].endswith('Z'):
                    try:
                        ts_dt = datetime.datetime.fromisoformat(r['yandex_instance_start_ts'].replace('Z', '+00:00'))
                        date_key = ts_dt.astimezone(msk_tz).strftime('%Y-%m-%d')
                    except: pass
                        
                if not date_key: continue
                    
                if eid_str not in recs_by_event: 
                    recs_by_event[eid_str] = {}
                    
                if r['recording_url']:
                    recs_by_event[eid_str][date_key] = {"url": r['recording_url']}
                    
    conn.close()
    
    series_dict = {}
    colors = ['#3788d8', '#28a745', '#dc3545', '#fd7e14', '#6f42c1', '#20c997']
    weekdays_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    
    for row in db_events:
        try:
            st = datetime.datetime.fromisoformat(row['start_time'].replace('Z', '+00:00')).astimezone(msk_tz)
            et = datetime.datetime.fromisoformat(row['end_time'].replace('Z', '+00:00')).astimezone(msk_tz)
        except: continue
        
        group_key = f"{row['event_name']}_{st.weekday()}_{st.hour}_{st.minute}"
        
        if group_key not in series_dict:
            color = colors[hash(row['event_name'].split(',')[0]) % len(colors)]
            series_dict[group_key] = {
                "group_key": group_key,
                "title": row['event_name'],
                "weekday_str": weekdays_ru[st.weekday()],
                "time_str": f"{st.strftime('%H:%M')} - {et.strftime('%H:%M')}",
                "color": color,
                "events": []
            }
        
        date_str = st.strftime('%Y-%m-%d')
        eid_str = str(row['event_id'])
        recordings_dict = {}
        
        if eid_str in recs_by_event and date_str in recs_by_event[eid_str]:
            recordings_dict[date_str] = recs_by_event[eid_str][date_str]
        
        series_dict[group_key]["events"].append({
            "id": f"{row['event_id']}_{row['instance_start_ts']}",
            "template_event_id": row['event_id'],
            "title": row['event_name'],
            "start": st.isoformat(),
            "end": et.isoformat(),
            "backgroundColor": series_dict[group_key]["color"],
            "borderColor": series_dict[group_key]["color"],
            "total_attendees": row['total_attendees'],
            "link_description": row['link_description'],
            "teacher_names": row['teacher_names'],
            "attendees_emails": row['attendees_emails'],
            "recordings": recordings_dict 
        })
        
    return list(series_dict.values())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8082)