"""Parse Yandex calendars for every member of a single TiMe channel.

Unlike parse_all_to_pg.py this does not rebuild the course table and never
truncates: the student roster is taken straight from one channel (by default
the university-wide off-topic chat) and events are upserted on top of whatever
is already in the database. That makes it safe to run when the course list is
out of date, e.g. right after a new academic year starts.
"""

import argparse
import time
from datetime import date, datetime, timedelta

import requests

from app.core.config import get_settings
from app.db import db_connection, init_db
from app.integrations.yandex_api import YandexCalendarAPI


DEFAULT_CHANNEL = "off-topic"
STUDENT_DOMAIN = "@edu.centraluniversity.ru"
CHUNK_DAYS = 200
SKIP_EVENT_NAMES = {"Занят", "Событие скрыто"}


def get_channel_id(channel_name: str) -> str | None:
    settings = get_settings()
    response = requests.get(
        f"https://time.cu.ru/api/v4/teams/{settings.time_team_id}/channels/name/{channel_name}",
        headers=settings.time_headers,
        timeout=30,
    )
    if response.status_code == 401:
        raise SystemExit(
            "TiMe вернул 401: сессия истекла. Обновите TIME_COOKIE и TIME_CSRF_TOKEN в .env."
        )
    if response.status_code != 200:
        raise SystemExit(f"Канал {channel_name} не найден: HTTP {response.status_code}")
    return response.json().get("id")


def fetch_members(channel_id: str) -> set[str]:
    settings = get_settings()
    emails: set[str] = set()
    page = 0
    while True:
        response = requests.get(
            f"https://time.cu.ru/api/v4/users?in_channel={channel_id}&page={page}&per_page=100",
            headers=settings.time_headers,
            timeout=30,
        )
        if response.status_code != 200:
            raise SystemExit(f"Не удалось получить участников канала: HTTP {response.status_code}")
        batch = response.json()
        if not batch:
            break
        for user in batch:
            if user.get("is_bot"):
                continue
            email = user.get("email", "").strip().lower()
            if not email:
                username = user.get("username", "").strip().lower()
                email = f"{username}{STUDENT_DOMAIN}" if username else ""
            if email.endswith(STUDENT_DOMAIN):
                emails.add(email)
        if len(batch) < 100:
            break
        page += 1
        time.sleep(1)
    return emails


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--channel", default=DEFAULT_CHANNEL, help="имя канала в TiMe")
    parser.add_argument("--start", type=date.fromisoformat, default=date.today(), help="YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, default=None, help="YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=14, help="сколько дней вперёд, если не задан --end")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="только собрать список студентов и выйти, ничего не записывая",
    )
    args = parser.parse_args()
    if args.end is None:
        args.end = args.start + timedelta(days=args.days)
    if args.start > args.end:
        parser.error("--start позже --end")
    return args


def main() -> None:
    args = parse_args()
    settings = get_settings()

    print(f"Канал: {args.channel}; период: {args.start} .. {args.end}")
    channel_id = get_channel_id(args.channel)
    students = sorted(fetch_members(channel_id))
    print(f"Студентов в канале: {len(students)}")
    if not students:
        raise SystemExit("В канале нет ни одного адреса на " + STUDENT_DOMAIN + ", выходим.")
    if args.dry_run:
        for email in students[:20]:
            print("  ", email)
        print("dry-run: в базу ничего не записано")
        return

    init_db()
    api = YandexCalendarAPI(cookie_path=str(settings.cookie_file))

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT email, full_name FROM teachers")
            teachers = {row[0].lower(): row[1] for row in cur.fetchall()}
            cur.execute("SELECT event_id, instance_start_ts FROM calendar_events")
            known_events = {(row[0], row[1]) for row in cur.fetchall()}

            for email in students:
                cur.execute(
                    "INSERT INTO students (email) VALUES (%s) ON CONFLICT (email) DO NOTHING",
                    (email,),
                )
            conn.commit()

            found_events: dict[tuple[str, str], dict] = {}
            links: list[tuple[str, str, str]] = []
            for index, email in enumerate(students, 1):
                if index % 25 == 0:
                    print(f"  календари: {index} / {len(students)}", flush=True)
                current_start = args.start
                while current_start <= args.end:
                    current_end = min(current_start + timedelta(days=CHUNK_DAYS), args.end)
                    for event in api.get_detailed_events_for_range(email, current_start, current_end):
                        event_id = str(event.get("event_id") or "")
                        if not event_id or event_id == "None":
                            continue
                        if event.get("name") in SKIP_EVENT_NAMES:
                            continue
                        key = (event_id, str(event.get("instance_start_ts")))
                        found_events.setdefault(key, event)
                        links.append((email, key[0], key[1]))
                    current_start = current_end + timedelta(days=1)
                    time.sleep(0.5)

            new_events = {key: event for key, event in found_events.items() if key not in known_events}
            print(f"Найдено пар: {len(found_events)}; из них новых: {len(new_events)}", flush=True)

            for index, (key, event) in enumerate(new_events.items(), 1):
                if index % 50 == 0:
                    print(f"  детали: {index} / {len(new_events)}", flush=True)
                link_desc, attendee_emails = api.get_event_details_by_id(key[0], key[1])
                matched = [teachers[a] for a in attendee_emails if a in teachers]
                cur.execute(
                    """
                    INSERT INTO calendar_events
                    (event_id, instance_start_ts, start_time, end_time, event_name,
                     total_attendees, link_description, attendees_emails, teacher_names)
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
                        ", ".join(matched) if matched else "Не определен",
                    ),
                )
            conn.commit()

            for email, event_id, instance_ts in links:
                cur.execute(
                    """
                    INSERT INTO student_calendar_link (student_email, event_id, instance_start_ts)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (student_email, event_id, instance_start_ts) DO NOTHING
                    """,
                    (email, event_id, instance_ts),
                )
        conn.commit()

    print(f"Готово в {datetime.now():%H:%M:%S}: студентов {len(students)}, связей {len(links)}")


if __name__ == "__main__":
    main()
