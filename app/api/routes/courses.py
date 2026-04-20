import asyncio
import datetime
from datetime import timedelta

import requests
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from psycopg2.extras import RealDictCursor

from app.auth import verify_admin, verify_user_or_admin
from app.core.config import get_settings
from app.db import db_connection
from app.integrations.yandex_api import YandexCalendarAPI
from app.schemas import CourseUpdateModel
from app.services.common import emit


router = APIRouter(prefix="/api/courses")


@router.get("")
async def get_courses(auth: dict = Depends(verify_user_or_admin)) -> list[dict]:
    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, track, course_year, level, name, search_query, link
                FROM courses
                ORDER BY track, course_year, name
                """
            )
            return [dict(row) for row in cur.fetchall()]


@router.post("/{course_id}")
async def update_course(course_id: int, data: CourseUpdateModel, admin: str = Depends(verify_admin)) -> dict[str, str]:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE courses SET search_query = %s, link = %s WHERE id = %s",
                (data.search_query.strip(), data.link.strip(), course_id),
            )
        conn.commit()
    return {"status": "ok"}


async def scan_generator(course_id: int):
    settings = get_settings()
    try:
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT link FROM courses WHERE id = %s", (course_id,))
                course = cur.fetchone()
        if not course or not course["link"]:
            yield emit("error", "У курса нет ссылки на TiMe")
            return

        link = course["link"].strip()
        channel_name = link.split("/")[-1]
        yield emit("progress", f"🔍 Поиск канала <b>{channel_name}</b>...")
        await asyncio.sleep(0.2)

        response = requests.get(
            f"https://time.cu.ru/api/v4/teams/{settings.time_team_id}/channels/name/{channel_name}",
            headers=settings.time_headers,
        )
        if response.status_code != 200:
            yield emit("error", "Канал не найден. Проверь куки/ссылку.")
            return

        channel_id = response.json().get("id")
        users = requests.get(
            f"https://time.cu.ru/api/v4/users?in_channel={channel_id}&page=0&per_page=100",
            headers=settings.time_headers,
        ).json()
        valid_emails = [
            user.get("email", "").strip().lower() or f"{user.get('username', '').lower()}@edu.centraluniversity.ru"
            for user in users
            if not user.get("is_bot")
        ]
        target_students = list(set(email for email in valid_emails if email.endswith("@edu.centraluniversity.ru")))[:50]
        if not target_students:
            yield emit("error", "В канале нет студентов")
            return

        yield emit("progress", f"✅ Найдено {len(target_students)} студентов. Сканируем их календари...")
        api = YandexCalendarAPI(cookie_path=str(settings.cookie_file))
        start_date = datetime.date.today()
        end_date = start_date + timedelta(days=14)
        found_names = set()
        for index, email in enumerate(target_students, 1):
            yield emit("progress", f"👀 Читаем расписание {index}/{len(target_students)}: <small>{email}</small>")
            await asyncio.sleep(0.1)
            for event in api.get_detailed_events_for_range(email, start_date, end_date):
                found_names.add(event["name"])
        yield emit("progress", "🎉 Сканирование завершено. Выберите нужное название ниже.")
        yield emit("done", {"names": list(found_names)})
    except Exception as exc:
        yield emit("error", f"Ошибка сканирования: {exc}")


@router.post("/{course_id}/scan")
async def scan_course_endpoint(course_id: int, admin: str = Depends(verify_admin)) -> StreamingResponse:
    return StreamingResponse(scan_generator(course_id), media_type="text/plain")


async def extract_generator(course_id: int, search_query: str):
    settings = get_settings()
    try:
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT name, link FROM courses WHERE id = %s", (course_id,))
                course = cur.fetchone()
            if not course:
                yield emit("error", "Курс не найден.")
                return

            link = course["link"].strip()
            channel_name = link.split("/")[-1]
            yield emit("progress", f"🚀 Ищем целевые пары по запросу: <b>{search_query}</b>...")
            await asyncio.sleep(0.2)

            channel_id = requests.get(
                f"https://time.cu.ru/api/v4/teams/{settings.time_team_id}/channels/name/{channel_name}",
                headers=settings.time_headers,
            ).json().get("id")
            users = requests.get(
                f"https://time.cu.ru/api/v4/users?in_channel={channel_id}&page=0&per_page=100",
                headers=settings.time_headers,
            ).json()
            valid_emails = [
                user.get("email", "").strip().lower() or f"{user.get('username', '').lower()}@edu.centraluniversity.ru"
                for user in users
                if not user.get("is_bot")
            ]
            target_students = list(set(email for email in valid_emails if email.endswith("@edu.centraluniversity.ru")))[:7]

            api = YandexCalendarAPI(cookie_path=str(settings.cookie_file))
            start_date = datetime.date.today()
            end_date = start_date + timedelta(days=14)
            unique_events_dict = {}
            student_links = []
            for email in target_students:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO students (email) VALUES (%s) ON CONFLICT (email) DO NOTHING", (email,))
                events = api.get_detailed_events_for_range(email, start_date, end_date)
                for event in events:
                    event_id = event.get("event_id")
                    if event_id and search_query.lower() in event["name"].lower():
                        event_key = (event_id, event.get("instance_start_ts"))
                        unique_events_dict[event_key] = event
                        student_links.append((email, event_key[0], event_key[1]))

            if not unique_events_dict:
                yield emit("error", "По такому запросу пар не найдено.")
                return

            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT email, full_name FROM teachers")
                teachers_dict = {row["email"].lower(): row["full_name"] for row in cur.fetchall()}

            yield emit("progress", f"🎯 Найдено пар по предмету: {len(unique_events_dict)}. Достаем ссылки...")
            with conn.cursor() as cur:
                for index, (event_key, event_data) in enumerate(unique_events_dict.items(), 1):
                    yield emit("progress", f"🔗 Парсинг деталей: {index} / {len(unique_events_dict)}...")
                    await asyncio.sleep(0.3)
                    link_desc, attendees_emails = api.get_event_details_by_id(event_key[0], event_key[1])
                    matched_teachers = [teachers_dict[email] for email in attendees_emails if email in teachers_dict]
                    cur.execute(
                        """
                        INSERT INTO calendar_events
                        (event_id, instance_start_ts, start_time, end_time, event_name, total_attendees, link_description, attendees_emails, teacher_names)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_id, instance_start_ts) DO UPDATE SET
                        link_description = excluded.link_description,
                        attendees_emails = excluded.attendees_emails,
                        teacher_names = excluded.teacher_names
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
                for link_tuple in student_links:
                    cur.execute(
                        """
                        INSERT INTO student_calendar_link (student_email, event_id, instance_start_ts)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (student_email, event_id, instance_start_ts) DO NOTHING
                        """,
                        link_tuple,
                    )
            conn.commit()
        yield emit("progress", "✅ Пары и ссылки успешно сохранены!")
        yield emit("done", {"count": len(unique_events_dict)})
    except Exception as exc:
        yield emit("error", f"Ошибка выгрузки: {exc}")


@router.post("/{course_id}/extract")
async def extract_course_endpoint(
    course_id: int,
    search_query: str = Query(...),
    admin: str = Depends(verify_admin),
) -> StreamingResponse:
    return StreamingResponse(extract_generator(course_id, search_query), media_type="text/plain")
