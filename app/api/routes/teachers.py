from typing import Optional

import psycopg2
from fastapi import APIRouter, Depends, Query
from psycopg2.extras import RealDictCursor

from app.auth import verify_admin, verify_user_or_admin
from app.db import db_connection
from app.schemas import NewTeacherModel


router = APIRouter()


@router.get("/api/teachers")
async def get_teachers(queries: Optional[list[str]] = Query(None), auth: dict = Depends(verify_user_or_admin)) -> dict:
    if not queries:
        return {}
    result = {}
    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for query in queries:
                if not query.strip() or query == "null":
                    continue
                cur.execute("SELECT DISTINCT teacher_names FROM calendar_events WHERE event_name LIKE %s", (f"%{query}%",))
                teacher_set = set()
                for row in cur.fetchall():
                    if row["teacher_names"]:
                        teacher_set.update(item.strip() for item in row["teacher_names"].split(",") if item.strip())
                teacher_list = sorted(teacher_set)
                if "Не определен" in teacher_list:
                    teacher_list.remove("Не определен")
                    teacher_list.append("Не определен")
                result[query] = teacher_list
    return result


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
