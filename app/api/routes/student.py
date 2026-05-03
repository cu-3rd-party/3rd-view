import datetime
from datetime import timedelta

from fastapi import APIRouter, Depends
from psycopg2.extras import RealDictCursor

from app.auth import get_current_user
from app.core.config import get_settings
from app.db import db_connection
from app.integrations.yandex_api import YandexCalendarAPI
from app.services.common import event_color


router = APIRouter()


@router.get("/api/student/import")
async def import_student_schedule(email: str, user: str = Depends(get_current_user)) -> dict:
    settings = get_settings()
    email = email.lower().strip()
    if not email.endswith("@edu.centraluniversity.ru"):
        return {"status": "error", "message": "Введите корпоративную почту (@edu.centraluniversity.ru)"}

    api = YandexCalendarAPI(cookie_path=str(settings.cookie_file))
    start_date = datetime.date.today() - timedelta(days=7)
    end_date = datetime.date.today() + timedelta(days=30)
    raw_events = api.get_detailed_events_for_range(email, start_date, end_date)
    if not raw_events:
        return {"status": "ok", "events": []}

    event_ids = list({str(event["event_id"]) for event in raw_events if event.get("event_id")})
    db_events_dict = {}
    recs_by_event = {}
    teachers_dict = {}
    msk_tz = datetime.timezone(datetime.timedelta(hours=3))

    with db_connection() as conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT email, full_name FROM teachers")
                teachers_dict = {row["email"].lower(): row["full_name"] for row in cur.fetchall()}
                if event_ids:
                    placeholders = ",".join(["%s"] * len(event_ids))
                    cur.execute(
                        f"""
                        SELECT event_id, instance_start_ts, total_attendees, link_description, teacher_names, attendees_emails
                        FROM calendar_events WHERE event_id IN ({placeholders})
                        """,
                        tuple(event_ids),
                    )
                    db_events_dict = {
                        (str(row["event_id"]), str(row["instance_start_ts"])): row
                        for row in cur.fetchall()
                    }
                    cur.execute(
                        f"""
                        SELECT yandex_event_id, yandex_instance_start_ts, recording_url, recording_date
                        FROM event_recordings WHERE yandex_event_id IN ({placeholders})
                        """,
                        tuple(event_ids),
                    )
                    for record in cur.fetchall():
                        date_key = record["recording_date"]
                        if not date_key and record["yandex_instance_start_ts"]:
                            try:
                                ts_dt = datetime.datetime.fromisoformat(record["yandex_instance_start_ts"].replace("Z", "+00:00"))
                                date_key = ts_dt.astimezone(msk_tz).strftime("%Y-%m-%d")
                            except Exception:
                                pass
                        if date_key and record["recording_url"]:
                            recs_by_event.setdefault(str(record["yandex_event_id"]), {})[date_key] = {"url": record["recording_url"]}
                    
                    try:
                        cur.execute(
                            f"""
                            SELECT yandex_event_id, yandex_instance_start_ts, suggested_url, suggested_by_email
                            FROM suggested_recordings WHERE yandex_event_id IN ({placeholders})
                            """,
                            tuple(event_ids),
                        )
                        for sug in cur.fetchall():
                            date_key = None
                            if sug["yandex_instance_start_ts"]:
                                try:
                                    ts_dt = datetime.datetime.fromisoformat(sug["yandex_instance_start_ts"].replace("Z", "+00:00"))
                                    date_key = ts_dt.astimezone(msk_tz).strftime("%Y-%m-%d")
                                except Exception:
                                    pass
                            if date_key and sug["suggested_url"]:
                                entry = recs_by_event.setdefault(str(sug["yandex_event_id"]), {}).setdefault(date_key, {})
                                entry["suggested_url"] = sug["suggested_url"]
                                entry["suggested_by_email"] = sug["suggested_by_email"]
                    except Exception:
                        pass
        except Exception:
            pass

        formatted_events = []
        with conn.cursor() as cur:
            for event in raw_events:
                if event["name"] in ["Занят", "Событие скрыто"]:
                    continue
                event_id = str(event.get("event_id", ""))
                instance = str(event.get("instance_start_ts", ""))
                db_data = db_events_dict.get((event_id, instance))
                if not db_data or not db_data.get("attendees_emails"):
                    link_desc, attendees_emails = api.get_event_details_by_id(event_id, instance)
                    matched_teachers = [teachers_dict[email] for email in attendees_emails if email in teachers_dict]
                    db_data = {
                        "total_attendees": event.get("total_attendees", len(attendees_emails)),
                        "link_description": link_desc,
                        "teacher_names": ", ".join(list(set(matched_teachers))) if matched_teachers else "Не определен",
                        "attendees_emails": ",".join(attendees_emails),
                    }
                    try:
                        cur.execute(
                            """
                            INSERT INTO calendar_events
                            (event_id, instance_start_ts, start_time, end_time, event_name, total_attendees, link_description, attendees_emails, teacher_names)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (event_id, instance_start_ts) DO UPDATE SET
                            link_description = excluded.link_description,
                            attendees_emails = excluded.attendees_emails,
                            teacher_names = excluded.teacher_names,
                            total_attendees = excluded.total_attendees
                            """,
                            (
                                event_id,
                                instance,
                                event["start"].isoformat(),
                                event["end"].isoformat(),
                                event["name"],
                                db_data["total_attendees"],
                                db_data["link_description"],
                                db_data["attendees_emails"],
                                db_data["teacher_names"],
                            ),
                        )
                        conn.commit()
                    except Exception:
                        conn.rollback()
                date_str = event["start"].astimezone(msk_tz).strftime("%Y-%m-%d")
                formatted_events.append(
                    {
                        "id": f"{event_id}_{instance}",
                        "template_event_id": event_id,
                        "title": event["name"],
                        "start": event["start"].isoformat(),
                        "end": event["end"].isoformat(),
                        "backgroundColor": event_color(event["name"]),
                        "borderColor": event_color(event["name"]),
                        "total_attendees": db_data.get("total_attendees", 0),
                        "link_description": db_data.get("link_description", ""),
                        "teacher_names": db_data.get("teacher_names", "Не определен"),
                        "attendees_emails": db_data.get("attendees_emails", ""),
                        "recordings": {date_str: recs_by_event.get(event_id, {}).get(date_str)} if recs_by_event.get(event_id, {}).get(date_str) else {},
                    }
                )
    return {"status": "ok", "events": formatted_events}
