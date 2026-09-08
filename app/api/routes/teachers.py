from typing import Optional

import psycopg2
from fastapi import APIRouter, Depends, Query
from psycopg2.extras import RealDictCursor

from app.auth import verify_admin, verify_user_or_admin
from app.db import db_connection
from app.schemas import NewTeacherModel, TeacherSearchRequest


router = APIRouter()


def _teachers_by_query(queries: list[str]) -> dict[str, list[str]]:
    """Для каждой строки поиска -- преподаватели пар, чьё название её содержит.

    Раньше здесь был отдельный SELECT ... LIKE на каждый запрос, а каталог вырос
    до ~900 предметов, так что один заход в фильтр стоил ~900 запросов в базу.
    Названий пар всего пара тысяч -- дешевле забрать их разом и сопоставить в питоне.
    """
    wanted = [q.strip() for q in queries if q and q.strip() and q != "null"]
    if not wanted:
        return {}

    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT DISTINCT event_name, teacher_names FROM calendar_events "
                "WHERE event_name IS NOT NULL AND teacher_names IS NOT NULL"
            )
            pairs = [(row["event_name"], row["teacher_names"]) for row in cur.fetchall()]

    result: dict[str, list[str]] = {}
    for query in wanted:
        if query in result:
            continue
        teacher_set: set[str] = set()
        for event_name, teacher_names in pairs:
            if query in event_name:
                teacher_set.update(item.strip() for item in teacher_names.split(",") if item.strip())
        teacher_list = sorted(teacher_set)
        if "Не определен" in teacher_list:
            teacher_list.remove("Не определен")
            teacher_list.append("Не определен")
        result[query] = teacher_list
    return result


@router.get("/api/teachers")
async def get_teachers(queries: Optional[list[str]] = Query(None), auth: dict = Depends(verify_user_or_admin)) -> dict:
    return _teachers_by_query(queries or [])


@router.post("/api/teachers/search")
async def search_teachers(data: TeacherSearchRequest, auth: dict = Depends(verify_user_or_admin)) -> dict:
    """То же самое, но списком в теле запроса.

    Каталог отдаёт сотни названий, а они длинные -- в query-строке это давало URL
    под 175 КБ, который уже ловил ERR_HTTP2_PROTOCOL_ERROR.
    """
    return _teachers_by_query(data.queries)


@router.post("/api/teachers")
async def add_teacher(data: NewTeacherModel, admin: str = Depends(verify_admin)) -> dict[str, str]:
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO teachers (email, full_name) VALUES (%s, %s)",
                    (data.email.lower().strip(), data.full_name.strip()),
                )
            conn.commit()
        return {"status": "ok"}
    except psycopg2.errors.UniqueViolation:
        return {"status": "error", "message": "Преподаватель с таким Email уже существует"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.post("/api/admin/recheck_teachers")
async def recheck_teachers(admin: str = Depends(verify_admin)) -> dict[str, str | int]:
    updated_count = 0
    try:
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT email, full_name FROM teachers WHERE email IS NOT NULL")
                teachers_dict = {
                    row["email"].strip().lower(): row["full_name"].strip()
                    for row in cur.fetchall()
                    if row.get("email")
                }
                cur.execute("SELECT event_id, instance_start_ts, attendees_emails, teacher_names FROM calendar_events")
                events = cur.fetchall()
                for event in events:
                    if not event["attendees_emails"]:
                        continue
                    emails = [email.strip().lower() for email in event["attendees_emails"].split(",")]
                    matched_teachers = [teachers_dict[email] for email in emails if email in teachers_dict]
                    new_teacher_names = ", ".join(list(set(matched_teachers))) if matched_teachers else "Не определен"
                    if new_teacher_names != event["teacher_names"]:
                        cur.execute(
                            """
                            UPDATE calendar_events
                            SET teacher_names = %s
                            WHERE event_id = %s AND instance_start_ts = %s
                            """,
                            (new_teacher_names, event["event_id"], event["instance_start_ts"]),
                        )
                        updated_count += 1
            conn.commit()
        return {"status": "ok", "updated": updated_count}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}
