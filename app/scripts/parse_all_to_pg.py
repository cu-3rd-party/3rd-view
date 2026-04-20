import csv
import time
from datetime import date, timedelta

import requests

from app.core.config import get_settings
from app.db import db_connection, init_db
from app.integrations.yandex_api import YandexCalendarAPI


CSV_PATH = "new_courses.csv"
DAYS_TO_PARSE = 14


def clean_database() -> None:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE courses, students, student_calendar_link, calendar_events RESTART IDENTITY CASCADE;")
        conn.commit()


def get_channel_id(channel_name: str) -> str | None:
    settings = get_settings()
    response = requests.get(
        f"https://time.cu.ru/api/v4/teams/{settings.time_team_id}/channels/name/{channel_name}",
        headers=settings.time_headers,
    )
    if response.status_code == 200:
        return response.json().get("id")
    return None


def fetch_users_in_channel(channel_id: str) -> list[dict]:
    settings = get_settings()
    users = []
    page = 0
    while True:
        response = requests.get(
            f"https://time.cu.ru/api/v4/users?in_channel={channel_id}&page={page}&per_page=100",
            headers=settings.time_headers,
        )
        if response.status_code != 200:
            break
        batch = response.json()
        users.extend(batch)
        if len(batch) < 100:
            break
        page += 1
        time.sleep(1)
    return users


def main() -> None:
    settings = get_settings()
    init_db()
    clean_database()
    with db_connection() as conn:
        with conn.cursor() as cur:
            courses_data = []
            with open(CSV_PATH, "r", encoding="utf-8-sig") as file_obj:
                reader = csv.DictReader(file_obj, delimiter=",")
                for row in reader:
                    link = row.get("Ссылка на канал в TiMe", "").strip()
                    if not link:
                        continue
                    channel_name = link.split("/")[-1]
                    courses_data.append(
                        {
                            "track": row.get("Направление", ""),
                            "course_year": row.get("Курс", ""),
                            "level": row.get("Уровень", ""),
                            "name": row.get("Название курса", ""),
                            "link": link,
                            "search_query": channel_name,
                        }
                    )
                    cur.execute(
                        """
                        INSERT INTO courses (track, course_year, level, name, search_query, link)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            row.get("Направление", ""),
                            row.get("Курс", ""),
                            row.get("Уровень", ""),
                            row.get("Название курса", ""),
                            channel_name,
                            link,
                        ),
                    )
            conn.commit()

            all_unique_students = set()
            for course in courses_data:
                channel_id = get_channel_id(course["search_query"])
                time.sleep(1)
                if not channel_id:
                    continue
                for user in fetch_users_in_channel(channel_id):
                    if user.get("is_bot"):
                        continue
                    email = user.get("email", "").strip().lower() or f"{user.get('username', '').strip().lower()}@edu.centraluniversity.ru"
                    if email.endswith("@edu.centraluniversity.ru"):
                        all_unique_students.add(email)
            for email in all_unique_students:
                cur.execute("INSERT INTO students (email) VALUES (%s) ON CONFLICT (email) DO NOTHING", (email,))
            conn.commit()

            api = YandexCalendarAPI(cookie_path=str(settings.cookie_file))
            start_date = date.today()
            end_date = start_date + timedelta(days=DAYS_TO_PARSE)
            unique_events = {}
            student_events = {}
            for email in all_unique_students:
                events = api.get_detailed_events_for_range(email, start_date, end_date)
                student_events[email] = []
                for event in events:
                    event_id = str(event.get("event_id"))
                    instance_ts = str(event.get("instance_start_ts"))
                    if not event_id or event_id == "None":
                        continue
                    key = (event_id, instance_ts)
                    unique_events.setdefault(key, event)
                    student_events[email].append(key)
                time.sleep(0.5)

            for key, event in unique_events.items():
                link_desc, attendee_emails = api.get_event_details_by_id(key[0], key[1])
                cur.execute(
                    """
                    INSERT INTO calendar_events
                    (event_id, instance_start_ts, start_time, end_time, event_name, total_attendees, link_description, attendees_emails, teacher_names)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id, instance_start_ts) DO UPDATE SET
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    event_name = EXCLUDED.event_name,
                    total_attendees = EXCLUDED.total_attendees,
                    link_description = EXCLUDED.link_description,
                    attendees_emails = EXCLUDED.attendees_emails
                    """,
                    (
                        key[0],
                        key[1],
                        event["start"].isoformat(),
                        event["end"].isoformat(),
                        event["name"],
                        event.get("total_attendees", 0),
                        link_desc,
                        ",".join(attendee_emails),
                        "Не определен",
                    ),
                )
            for email, keys in student_events.items():
                for key in keys:
                    cur.execute(
                        """
                        INSERT INTO student_calendar_link (student_email, event_id, instance_start_ts)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (student_email, event_id, instance_start_ts) DO NOTHING
                        """,
                        (email, key[0], key[1]),
                    )
        conn.commit()


if __name__ == "__main__":
    main()
