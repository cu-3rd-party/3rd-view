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


# === ФУНКЦИЯ ДЛЯ УМНОГО СРАВНЕНИЯ НАЗВАНИЙ ===
def is_similar_title(t1: str, t2: str) -> bool:
    words1 = set(re.findall(r'[а-яa-z]{4,}', t1.lower()))
    words2 = set(re.findall(r'[а-яa-z]{4,}', t2.lower()))
    return len(words1.intersection(words2)) > 0


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
    # Безопасный парсинг для получения даты по Москве
    if start_time_iso.endswith("Z"):
        start_time_iso = start_time_iso.replace("Z", "+00:00")
        
    date_str = datetime.fromisoformat(start_time_iso).astimezone(
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
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            # Ищем пары с окном от -12 до +24 часов, так мы точно не пропустим ничего из-за часовых поясов
            start_bound = (now_utc - timedelta(hours=12)).isoformat()
            end_bound = (now_utc + timedelta(hours=24)).isoformat()

            with db_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Убрали кривой LIKE, теперь берем по диапазону
                    cur.execute(
                        "SELECT event_id, instance_start_ts, start_time, event_name, link_description "
                        "FROM calendar_events WHERE start_time >= %s AND start_time <= %s",
                        (start_bound, end_bound),
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
                    ts_str = event["start_time"]
                    if ts_str.endswith("Z"):
                        ts_str = ts_str.replace("Z", "+00:00")
                    start_time = datetime.fromisoformat(ts_str)
                except Exception:
                    continue
                    
                match = KTALK_LINK_PATTERN.search(event["link_description"] or "")
                if not match:
                    continue
                    
                target_time = start_time + timedelta(seconds=20)
                delta_seconds = (target_time - now).total_seconds()
                
                # Если до пары от -5 до +10 минут
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
    msk_tz = timezone(timedelta(hours=3))
    settings = get_settings()

    while True:
        try:
            ktalk = KTalkAPI(auth_file_path=str(settings.ktalk_auth_file))
            # Берем больше страниц для надежности (5 -> 10)
            recordings = [
                conference
                for conference in ktalk.get_all_history_records(max_pages=10)
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
                                # Безопасный парсинг времени Толка
                                ts_str = rec["start_time"]
                                if ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("Z", "+00:00")
                                rec_dt = datetime.fromisoformat(ts_str).astimezone(msk_tz)
                            except Exception:
                                continue
                            
                            rec_title = f"{rec.get('title', '')} {rec.get('room_name', '')}".lower()

                            best_match = None
                            min_diff = 2.0  # Максимальное расхождение - 2 часа

                            for db_event in db_events:
                                try:
                                    y_ts_str = db_event["start_time"]
                                    if y_ts_str.endswith("Z"):
                                        y_ts_str = y_ts_str.replace("Z", "+00:00")
                                    yandex_dt = datetime.fromisoformat(y_ts_str).astimezone(msk_tz)
                                except Exception:
                                    continue
                                
                                diff_hours = abs((rec_dt - yandex_dt).total_seconds()) / 3600.0
                                
                                # Проверяем, что разница < 2 часов И названия пересекаются
                                if diff_hours < min_diff and is_similar_title(db_event["event_name"], rec_title):
                                    min_diff = diff_hours
                                    best_match = db_event

                            if best_match:
                                cur.execute(
                                    """
                                    INSERT INTO event_recordings
                                    (yandex_event_id, yandex_instance_start_ts, ktalk_id, recording_url, recording_date)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (yandex_event_id, yandex_instance_start_ts) DO UPDATE SET
                                    recording_url = CASE 
                                        WHEN event_recordings.recording_url LIKE '%' || EXCLUDED.recording_url || '%' THEN event_recordings.recording_url 
                                        ELSE event_recordings.recording_url || ',' || EXCLUDED.recording_url 
                                    END,
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
            
        # Запускаем синхронизацию записей раз в 90 минут (вместо привязки к конкретным минутам)
        await asyncio.sleep(5400) 


async def main() -> None:
    init_db()
    await asyncio.gather(bot_join_loop(), auto_sync_loop())


if __name__ == "__main__":
    asyncio.run(main())