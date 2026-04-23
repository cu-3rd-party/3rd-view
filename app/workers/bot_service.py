import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone

from psycopg2.extras import RealDictCursor

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db import db_connection, init_db
from app.integrations.ktalk_api import KTalkAPI
from app.integrations.ktalk_bot import connect


configure_logging()
logger = logging.getLogger("bot_service")
bot_lock = asyncio.Lock()
KTALK_LINK_PATTERN = re.compile(r"https://centraluniversity\.ktalk\.ru/[a-zA-Z0-9]+")


def is_visited(event_id: str, instance_start_ts: str) -> bool:
    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT bot_visited_at FROM event_recordings WHERE yandex_event_id = %s AND yandex_instance_start_ts = %s",
                (event_id, instance_start_ts),
            )
            row = cur.fetchone()
            return bool(row and row["bot_visited_at"])


def mark_as_visited(event_id: str, instance_start_ts: str, start_time_iso: str) -> None:
    visited_time = datetime.now(timezone.utc).isoformat()
    date_str = datetime.fromisoformat(start_time_iso.replace("Z", "+00:00")).astimezone(
        timezone(timedelta(hours=3))
    ).strftime("%Y-%m-%d")
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO event_recordings (yandex_event_id, yandex_instance_start_ts, recording_date, bot_visited_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (yandex_event_id, yandex_instance_start_ts)
                DO UPDATE SET bot_visited_at = EXCLUDED.bot_visited_at
                """,
                (event_id, instance_start_ts, date_str, visited_time),
            )
        conn.commit()


async def join_event_task(
    event_id: str,
    instance_start_ts: str,
    start_time_iso: str,
    target_time: datetime,
    link: str,
    event_name: str,
) -> None:
    delay = (target_time - datetime.now(timezone.utc)).total_seconds()
    if delay > 0:
        logger.info("Waiting %s seconds for event %s", int(delay), event_name)
        await asyncio.sleep(delay)
    if is_visited(event_id, instance_start_ts):
        return
    async with bot_lock:
        if is_visited(event_id, instance_start_ts):
            return
        settings = get_settings()
        try:
            await asyncio.to_thread(connect, link, 10, str(settings.ktalk_auth_file))
            mark_as_visited(event_id, instance_start_ts, start_time_iso)
            logger.info("Bot completed event %s", event_name)
        except Exception:
            logger.exception("Bot failed for event %s", event_name)
        await asyncio.sleep(3)


async def bot_join_loop() -> None:
    active_tasks: set[str] = set()
    msk_tz = timezone(timedelta(hours=3))
    while True:
        try:
            today_str = datetime.now(msk_tz).strftime("%Y-%m-%d")
            with db_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SELECT event_id, instance_start_ts, start_time, event_name, link_description FROM calendar_events WHERE start_time LIKE %s",
                        (f"%{today_str}%",),
                    )
                    events = cur.fetchall()
            now = datetime.now(timezone.utc)
            for event in events:
                event_id = str(event["event_id"])
                instance_start_ts = str(event["instance_start_ts"])
                task_key = f"{event_id}_{instance_start_ts}"
                if task_key in active_tasks or is_visited(event_id, instance_start_ts):
                    continue
                try:
                    start_time = datetime.fromisoformat(event["start_time"].replace("Z", "+00:00"))
                except Exception:
                    continue
                match = KTALK_LINK_PATTERN.search(event["link_description"] or "")
                if not match:
                    continue
                target_time = start_time + timedelta(seconds=20)
                delta_seconds = (target_time - now).total_seconds()
                if -300 <= delta_seconds <= 600:
                    active_tasks.add(task_key)
                    task = asyncio.create_task(
                        join_event_task(
                            event_id,
                            instance_start_ts,
                            event["start_time"],
                            target_time,
                            match.group(0),
                            event["event_name"],
                        )
                    )
                    task.add_done_callback(lambda _task, key=task_key: active_tasks.discard(key))
        except Exception:
            logger.exception("Bot join loop failed")
        await asyncio.sleep(60)

async def auto_sync_loop() -> None:
    target_times = {"11:25", "12:55", "14:25", "15:55", "17:25", "18:55", "20:25", "21:55"}
    msk_tz = timezone(timedelta(hours=3))
    settings = get_settings()

    while True:
        now_msk = datetime.now(msk_tz)
        if now_msk.strftime("%H:%M") not in target_times:
            await asyncio.sleep(20)
            continue
        try:
            ktalk = KTalkAPI(auth_file_path=str(settings.ktalk_auth_file))
            recordings = [
                conference
                for conference in ktalk.get_all_history_records(max_pages=5)
                if conference.get("has_recording") and conference.get("recording_url")
            ]
            if recordings:
                with db_connection() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT event_id, instance_start_ts, start_time, event_name FROM calendar_events")
                        db_events = cur.fetchall()
                    
                    matched = 0
                    with conn.cursor() as cur:
                        for rec in recordings:
                            try:
                                rec_dt = datetime.fromisoformat(rec["start_time"].replace("Z", "").split(".")[0] + "+00:00").astimezone(msk_tz)
                            except Exception:
                                continue
                            
                            rec_title = f"{rec.get('title', '')} {rec.get('room_name', '')}".lower()

                            best_match = None
                            min_diff = 2.0

                            for db_event in db_events:
                                try:
                                    # ИСПОЛЬЗУЕМ start_time ДЛЯ МАТЕМАТИКИ
                                    yandex_dt = datetime.fromisoformat(db_event["start_time"].replace("Z", "+00:00")).astimezone(msk_tz)
                                except Exception:
                                    continue
                                
                                if rec_dt.date() != yandex_dt.date():
                                    continue
                                
                                if rec_title in db_event["event_name"].lower() or db_event["event_name"].lower() in rec_title:
                                    diff_hours = abs((rec_dt - yandex_dt).total_seconds()) / 3600.0
                                    if diff_hours < min_diff:
                                        min_diff = diff_hours
                                        best_match = db_event

                            if best_match:
                                cur.execute(
                                    """
                                    INSERT INTO event_recordings
                                    (yandex_event_id, yandex_instance_start_ts, ktalk_id, recording_url, recording_date)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (yandex_event_id, yandex_instance_start_ts) DO UPDATE SET
                                    recording_url = EXCLUDED.recording_url,
                                    ktalk_id = EXCLUDED.ktalk_id
                                    """,
                                    (
                                        best_match["event_id"],
                                        best_match["instance_start_ts"],
                                        rec["recording_id"],
                                        rec["recording_url"],
                                        rec_dt.strftime("%Y-%m-%d"),
                                    ),
                                )
                                matched += 1
                    conn.commit()
                    logger.info("Recording sync matched %s rows", matched)
        except Exception:
            logger.exception("Auto sync loop failed")
        await asyncio.sleep(61)

async def main() -> None:
    init_db()
    await asyncio.gather(bot_join_loop(), auto_sync_loop())


if __name__ == "__main__":
    asyncio.run(main())
