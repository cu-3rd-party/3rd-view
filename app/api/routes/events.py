import asyncio
import datetime
from datetime import timedelta
from typing import Optional

import requests
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from psycopg2.extras import RealDictCursor

from app.auth import verify_admin, verify_user_or_admin
from app.core.config import get_settings
from app.db import db_connection
from app.integrations.yandex_api import YandexCalendarAPI
from app.schemas import EventsRequest
from app.services.common import emit, event_color


router = APIRouter()


@router.post("/api/events")
async def get_events(data: EventsRequest, auth: dict = Depends(verify_user_or_admin)) -> list[dict]:
    if not data.filters:
        return []

    query = """
        SELECT start_time, end_time, event_name as title,
               total_attendees, link_description, teacher_names, attendees_emails, event_id, instance_start_ts
        FROM calendar_events
        WHERE event_name != 'Событие скрыто' AND event_name != 'Занят'
    """
    params: list[str] = []
    course_clauses = []
    for filter_item in data.filters:
        if not filter_item.teachers:
            continue
        t_clauses = " OR ".join(["teacher_names LIKE %s" for _ in filter_item.teachers])
        course_clauses.append(f"(event_name LIKE %s AND ({t_clauses}))")
        params.append(f"%{filter_item.query}%")
        params.extend(f"%{teacher}%" for teacher in filter_item.teachers)
    if not course_clauses:
        return []

    query += f" AND ({' OR '.join(course_clauses)})"
    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, tuple(params))
            events_raw = cur.fetchall()
        if not events_raw:
            return []

        event_ids = list({str(row["event_id"]) for row in events_raw})
        placeholders = ",".join(["%s"] * len(event_ids))
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT yandex_event_id, yandex_instance_start_ts, recording_url, recording_date
                FROM event_recordings
                WHERE yandex_event_id IN ({placeholders})
                """,
                tuple(event_ids),
            )
            recs_raw = cur.fetchall()

    recs_by_event: dict[str, dict[str, dict[str, str]]] = {}
    msk_tz = datetime.timezone(datetime.timedelta(hours=3))
    for record in recs_raw:
        event_id = str(record["yandex_event_id"])
        date_key = str(record["recording_date"])[:10] if record["recording_date"] else None
        if not date_key and record["yandex_instance_start_ts"] and record["yandex_instance_start_ts"].endswith("Z"):
            try:
                ts_dt = datetime.datetime.fromisoformat(record["yandex_instance_start_ts"].replace("Z", "+00:00"))
                date_key = ts_dt.astimezone(msk_tz).strftime("%Y-%m-%d")
            except Exception:
                pass
        if date_key and record["recording_url"]:
            recs_by_event.setdefault(event_id, {})[date_key] = {"url": record["recording_url"]}

    absolute_events = []
    for row in events_raw:
        try:
            start_time = datetime.datetime.fromisoformat(row["start_time"].replace("Z", "+00:00"))
            end_time = datetime.datetime.fromisoformat(row["end_time"].replace("Z", "+00:00"))
        except Exception:
            continue
        date_str = start_time.astimezone(msk_tz).strftime("%Y-%m-%d")
        event_id = str(row["event_id"])
        absolute_events.append(
            {
                "id": f"{event_id}_{row.get('instance_start_ts', '')}",
                "template_event_id": event_id,
                "title": row["title"],
                "start": start_time.replace(tzinfo=None).isoformat(),
                "end": end_time.replace(tzinfo=None).isoformat(),
                "backgroundColor": event_color(row["title"]),
                "borderColor": event_color(row["title"]),
                "total_attendees": row["total_attendees"],
                "link_description": row["link_description"],
                "teacher_names": row["teacher_names"],
                "attendees_emails": row["attendees_emails"],
                "recordings": {date_str: recs_by_event.get(event_id, {}).get(date_str)} if recs_by_event.get(event_id, {}).get(date_str) else {},
            }
        )
    return absolute_events


async def total_sync_generator(start_date: datetime.date, end_date: datetime.date):
    settings = get_settings()
    
    if start_date > end_date:
        yield emit("error", "Дата начала позже даты окончания!")
        return

    try:
        yield emit("progress", f"🚀 Старт тотальной синхронизации: с {start_date} по {end_date}...")
        api = YandexCalendarAPI(cookie_path=str(settings.cookie_file))
        
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. Достаем преподавателей и ссылки курсов
                cur.execute("SELECT email, full_name FROM teachers")
                teachers_dict = {row["email"].lower(): row["full_name"] for row in cur.fetchall()}
                cur.execute("SELECT link FROM courses WHERE link IS NOT NULL AND link != ''")
                courses = cur.fetchall()
                
                # 2. Выгружаем УЖЕ СУЩЕСТВУЮЩИЕ пары из БД, чтобы не перезаписывать их
                # и не тратить время на запросы их деталей в Яндекс
                cur.execute("SELECT event_id, instance_start_ts FROM calendar_events")
                existing_events = set((row["event_id"], row["instance_start_ts"]) for row in cur.fetchall())

            all_unique_students = set()
            yield emit("progress", "🕵️ Сбор базы студентов из каналов TiMe...")
            
            # --- Блок сбора студентов (остается без изменений) ---
            for course in courses:
                channel_name = course["link"].strip().split("/")[-1]
                response = requests.get(
                    f"https://time.cu.ru/api/v4/teams/{settings.time_team_id}/channels/name/{channel_name}",
                    headers=settings.time_headers,
                )
                if response.status_code != 200:
                    continue
                channel_id = response.json().get("id")
                page = 0
                while True:
                    chunk = requests.get(
                        f"https://time.cu.ru/api/v4/users?in_channel={channel_id}&page={page}&per_page=100",
                        headers=settings.time_headers,
                    ).json()
                    if not chunk:
                        break
                    valid_emails = [
                        user.get("email", "").strip().lower() or f"{user.get('username', '').lower()}@edu.centraluniversity.ru"
                        for user in chunk
                        if not user.get("is_bot")
                    ]
                    all_unique_students.update(email for email in valid_emails if email.endswith("@edu.centraluniversity.ru"))
                    page += 1
                    await asyncio.sleep(0.1)
            # ----------------------------------------------------

            students_list = list(all_unique_students)
            yield emit("progress", f"👥 Собрано уникальных студентов: {len(students_list)}")
            
            unique_events_dict = {}
            student_links = []
            
            with conn.cursor() as cur:
                # Проходим по студентам и собираем сетку календарей
                for index, email in enumerate(students_list, 1):
                    if index % 5 == 0:
                        yield emit("progress", f"📅 Проверяем календари: {index} / {len(students_list)}")
                    cur.execute("INSERT INTO students (email) VALUES (%s) ON CONFLICT (email) DO NOTHING", (email,))
                    
                    # ЧАНКИРОВАНИЕ (разбиваем период на отрезки по 30 дней)
                    current_start = start_date
                    while current_start <= end_date:
                        current_end = min(current_start + timedelta(days=200), end_date)
                        
                        # Запрашиваем Яндекс кусочками
                        events_chunk = api.get_detailed_events_for_range(email, current_start, current_end)
                        
                        for event in events_chunk:
                            if not event.get("event_id") or event["name"] in ["Занят", "Событие скрыто"]:
                                continue
                            event_key = (event["event_id"], event.get("instance_start_ts"))
                            unique_events_dict.setdefault(event_key, event)
                            student_links.append((email, event_key[0], event_key[1]))
                        
                        current_start = current_end + timedelta(days=1)
                        await asyncio.sleep(0.1)

                # Исключаем из запросов в Яндекс те пары, которые уже есть в нашей БД!
                events_to_fetch = {k: v for k, v in unique_events_dict.items() if k not in existing_events}
                
                yield emit("progress", f"🎯 Найдено уникальных пар (всего): {len(unique_events_dict)}")
                yield emit("progress", f"⚡ Из них НОВЫХ (надо скачать): {len(events_to_fetch)}")

                # Скачиваем детали ТОЛЬКО для новых пар
                for index, (event_key, event_data) in enumerate(events_to_fetch.items(), 1):
                    if index % 10 == 0:
                        yield emit("progress", f"🔗 Скачано деталей: {index} / {len(events_to_fetch)}")
                        
                    link_desc, attendees_emails = api.get_event_details_by_id(event_key[0], event_key[1])
                    matched_teachers = [teachers_dict[email] for email in attendees_emails if email in teachers_dict]
                    
                    cur.execute(
                        """
                        INSERT INTO calendar_events
                        (event_id, instance_start_ts, start_time, end_time, event_name, total_attendees, link_description, attendees_emails, teacher_names)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_id, instance_start_ts) DO NOTHING
                        """,
                        (
                            event_key[0],
                            event_key[1],
                            event_data["start"].isoformat(),
                            event_data["end"].isoformat(),
                            event_data["name"],
                            event_data["total_attendees"],
                            link_desc,
                            ",".join(attendees_emails),
                            ", ".join(list(set(matched_teachers))) if matched_teachers else "Не определен",
                        ),
                    )
                    await asyncio.sleep(0.2)
                
                yield emit("progress", "🧠 Привязываем студентов к парам (и к старым, и к новым)...")
                
                # Привязка студентов делается для всех найденных пар (ON CONFLICT игнорирует дубликаты)
                for link_tuple in student_links:
                    cur.execute(
                        """
                        INSERT INTO student_calendar_link (student_email, event_id, instance_start_ts)
                        VALUES (%s, %s, %s) ON CONFLICT (student_email, event_id, instance_start_ts) DO NOTHING
                        """,
                        link_tuple,
                    )
            conn.commit()
            
        yield emit("done", {"message": f"🎉 Тотальная синхронизация завершена! Добавлено новых пар: {len(events_to_fetch)}."})
        
    except Exception as exc:
        yield emit("error", f"Ошибка: {exc}")


@router.post("/api/admin/total_sync")
async def run_total_sync_endpoint(
    start_date: datetime.date = Query(...),
    end_date: datetime.date = Query(...),
    admin: str = Depends(verify_admin)
) -> StreamingResponse:
    return StreamingResponse(total_sync_generator(start_date, end_date), media_type="text/plain")