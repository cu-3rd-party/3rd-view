import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone

from psycopg2.extras import RealDictCursor

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db import db_connection, init_db
from app.integrations.ktalk_bot import connect

configure_logging()
logger = logging.getLogger("emergency_bot")
KTALK_LINK_PATTERN = re.compile(r"https://centraluniversity\.ktalk\.ru/[a-zA-Z0-9]+")


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


async def main() -> None:
    init_db()
    msk_tz = timezone(timedelta(hours=3))
    today_str = datetime.now(msk_tz).strftime("%Y-%m-%d")
    settings = get_settings()

    logger.info("Начинаю ПРИНУДИТЕЛЬНЫЙ экстренный поиск пар на 10:00...")

    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT event_id, instance_start_ts, start_time, event_name, link_description FROM calendar_events WHERE start_time LIKE %s",
                (f"%{today_str}%",),
            )
            events = cur.fetchall()

    for event in events:
        try:
            start_time = datetime.fromisoformat(event["start_time"].replace("Z", "+00:00")).astimezone(msk_tz)
        except Exception:
            continue

        if start_time.hour == 13 and start_time.minute < 15:
            event_id = str(event["event_id"])
            instance_start_ts = str(event["instance_start_ts"])
            event_name = event["event_name"]

            match = KTALK_LINK_PATTERN.search(event["link_description"] or "")
            if not match:
                logger.info(f"Нет ссылки на KTalk для {event_name}")
                continue

            link = match.group(0)
            logger.info(f"ПРИНУДИТЕЛЬНО захожу на пару: {event_name} по ссылке {link} ...")

            try:
                # Подключаемся без всяких проверок
                await asyncio.to_thread(connect, link, 10, str(settings.ktalk_auth_file))
                
                # Обновляем время посещения в БД
                mark_as_visited(event_id, instance_start_ts, event["start_time"])
                logger.info(f"Успешно подключился к {event_name}!")
            except Exception:
                logger.exception(f"Не удалось подключиться к {event_name}")
            
            # Небольшая пауза между подключениями
            await asyncio.sleep(3)

    logger.info("Экстренный скрипт завершил работу. Можно запускать основного бота.")

if __name__ == "__main__":
    asyncio.run(main())
