# Файл: bot_service.py
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from database import get_db_connection
from integrations.ktalk_api import KTalkAPI
from integrations.ktalk_bot import connect
from psycopg2.extras import RealDictCursor
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - BOT_SERVICE - %(levelname)s - %(message)s')
logger = logging.getLogger("bot_service")
bot_lock = asyncio.Lock()
KTALK_LINK_PATTERN = re.compile(r"https://centraluniversity\.ktalk\.ru/[a-zA-Z0-9]+")

def is_visited(event_id, instance_start_ts):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT bot_visited_at FROM event_recordings WHERE yandex_event_id = %s AND yandex_instance_start_ts = %s",
                        (event_id, instance_start_ts))
            row = cur.fetchone()
        return bool(row and row["bot_visited_at"])
    finally:
        conn.close()

def mark_as_visited(event_id, instance_start_ts, start_time_iso):
    conn = get_db_connection()
    try:
        st = datetime.fromisoformat(start_time_iso.replace('Z', '+00:00'))
        date_str = st.astimezone(timezone(timedelta(hours=3))).strftime('%Y-%m-%d')
        visited_time = datetime.now(timezone.utc).isoformat()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO event_recordings (yandex_event_id, yandex_instance_start_ts, recording_date, bot_visited_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (yandex_event_id, yandex_instance_start_ts) 
                DO UPDATE SET bot_visited_at = EXCLUDED.bot_visited_at
            """, (event_id, instance_start_ts, date_str, visited_time))
        conn.commit()
    finally:
        conn.close()

async def join_event_task(event_id, instance_start_ts, start_time_iso, target_time: datetime, link: str, event_name: str):
    """Асинхронная задача: ждет нужного времени, затем встает в очередь и запускает бота"""
    now = datetime.now(timezone.utc)
    delay = (target_time - now).total_seconds()
    
    if delay > 0:
        logger.info(f"⏳ Ожидание {int(delay)} сек. до пары: {event_name}")
        await asyncio.sleep(delay)
    
    if is_visited(event_id, instance_start_ts):
        return

    logger.info(f"🕒 Время пришло! Встаю в очередь на запуск: {event_name}")
    
    # === ЗДЕСЬ ЗАДАЧИ ВЫСТРАИВАЮТСЯ В ОЧЕРЕДЬ ===
    async with bot_lock:
        # Двойная проверка: пока мы стояли в очереди, кто-то мог уже отметить? (на всякий случай)
        if is_visited(event_id, instance_start_ts):
            return

        logger.info(f"🚀 Моя очередь! Запускаю бота на пару: {event_name}")
        try:
            # Запускаем бота на 10 секунд
            await asyncio.to_thread(connect, link, 10, "ktalk_auth.txt")
            
            # Отмечаем
            mark_as_visited(event_id, instance_start_ts, start_time_iso)
            logger.info(f"✅ Бот отсидел и вышел. Отметка добавлена: {event_name}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при запуске бота на {event_name}: {e}")
        
        # Делаем паузу в 3 секунды перед тем, как отдать очередь следующему боту (чтобы браузер успел убить процессы)
        await asyncio.sleep(3)

async def bot_join_loop():
    """Главный цикл, который проверяет календарь каждую минуту"""
    logger.info("🤖 Модуль авто-входа запущен! (Режим: строго по одному боту)")
    active_tasks = set()
    
    # Задаем московский часовой пояс
    msk_tz = timezone(timedelta(hours=3))

    while True:
        try:
            conn = get_db_connection()
            # Берем дату по МОСКВЕ, а не по Гринвичу!
            today_str = datetime.now(msk_tz).strftime("%Y-%m-%d")
            
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Читаем базу данных ЗАНОВО каждую минуту (если добавится новая пара - бот её увидит)
                cur.execute(
                    "SELECT event_id, instance_start_ts, start_time, event_name, link_description FROM calendar_events WHERE start_time LIKE %s",
                    (f"%{today_str}%",)
                )
                events = cur.fetchall()
            conn.close()

            now = datetime.now(timezone.utc)

            for ev in events:
                event_id = str(ev["event_id"])
                instance_start_ts = str(ev["instance_start_ts"])
                task_key = f"{event_id}_{instance_start_ts}"
                
                if task_key in active_tasks or is_visited(event_id, instance_start_ts):
                    continue

                try:
                    start_time = datetime.fromisoformat(ev["start_time"].replace('Z', '+00:00'))
                except: continue
                
                desc = ev["link_description"] or ""
                match = KTALK_LINK_PATTERN.search(desc)
                if not match: 
                    # Если ссылки нет, бот туда не пойдет
                    continue
                
                link = match.group(0)

                # Бот заходит через 20 секунд ПОСЛЕ официального старта пары
                target_time = start_time + timedelta(seconds=20)
                delta_seconds = (target_time - now).total_seconds()

                # Если до пары осталось меньше 10 минут (600 сек) и она началась не более 5 минут назад (-300 сек)
                if -300 <= delta_seconds <= 600:
                    active_tasks.add(task_key)
                    logger.info(f"👀 Обнаружена новая пара: {ev['event_name']}. Готовлюсь ко входу...")
                    
                    task = asyncio.create_task(
                        join_event_task(event_id, instance_start_ts, ev["start_time"], target_time, link, ev["event_name"])
                    )
                    task.add_done_callback(lambda t, k=task_key: active_tasks.discard(k))

        except Exception as e:
            logger.error(f"⚠️ Ошибка в цикле входа: {e}")

        # Цикл спит 60 секунд. Каждую минуту он делает СВЕЖИЙ запрос в базу.
        await asyncio.sleep(60)

async def auto_sync_loop():
    """Фоновый процесс: проверяет KTalk строго по расписанию (Московское время)"""
    if not KTalkAPI:
        logger.warning("KTalkAPI не найден в app.py. Фоновая синхронизация видео отключена.")
        return

    logger.info("🔄 Модуль фоновой синхронизации записей запущен (по расписанию)!")
    
    # Точное время, когда нужно проверять KTalk (по Москве)
    target_times = {"11:25", "12:55", "14:25", "15:55", "17:25", "18:55", "20:25", "21:55"}
    msk_tz = timezone(timedelta(hours=3))

    while True:
        # Получаем текущее время по Москве (часы:минуты)
        now_msk = datetime.now(msk_tz)
        current_time_str = now_msk.strftime("%H:%M")

        # Если время совпало с одним из целевых
        if current_time_str in target_times:
            logger.info(f"⏰ Наступило время {current_time_str}! Запускаю авто-синхронизацию KTalk...")
            try:
                ktalk = KTalkAPI()
                conferences = ktalk.get_all_history_records(max_pages=5)
                # Фильтруем те, где реально есть запись
                recordings = [conf for conf in conferences if conf.get('has_recording') and conf.get('recording_url')]
                
                if recordings:
                    conn = get_db_connection()
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT event_id, start_time, event_name FROM calendar_events")
                        db_events = cur.fetchall()
                    
                    matched = 0
                    with conn.cursor() as cur:
                        for rec in recordings:
                            raw_date = rec['start_time'].replace('Z', '')
                            clean_date = raw_date.split('.')[0] + '+00:00'
                            try: 
                                rec_dt = datetime.fromisoformat(clean_date).astimezone(msk_tz)
                            except: 
                                continue
                            
                            rec_date_str = rec_dt.strftime('%Y-%m-%d')
                            rec_title = f"{rec.get('title', '')} {rec.get('room_name', '')}".lower()

                            for dbe in db_events:
                                try: 
                                    dbe_dt = datetime.fromisoformat(dbe['start_time'].replace('Z', '+00:00')).astimezone(msk_tz)
                                except: 
                                    continue

                                if (rec_title in dbe['event_name'].lower() or dbe['event_name'].lower() in rec_title) and rec_dt.weekday() == dbe_dt.weekday():
                                    rec_hours = rec_dt.hour + rec_dt.minute / 60.0
                                    dbe_hours = dbe_dt.hour + dbe_dt.minute / 60.0
                                    
                                    if abs(rec_hours - dbe_hours) <= 2.0:
                                        fake_ts = f"ktalk_{rec['recording_id']}"
                                        # ЗАЛИВАЕМ URL-ЗАПИСИ В БД
                                        cur.execute("""
                                            INSERT INTO event_recordings 
                                            (yandex_event_id, yandex_instance_start_ts, ktalk_id, recording_url, recording_date)
                                            VALUES (%s, %s, %s, %s, %s)
                                            ON CONFLICT (yandex_event_id, yandex_instance_start_ts) DO UPDATE SET
                                            recording_url = EXCLUDED.recording_url,
                                            ktalk_id = EXCLUDED.ktalk_id
                                        """, (dbe['event_id'], fake_ts, rec['recording_id'], rec['recording_url'], rec_date_str))
                                        matched += 1
                                        break

                    conn.commit()
                    conn.close()
                    logger.info(f"✅ Авто-синхронизация завершена. Добавлено/обновлено записей: {matched}.")
                else:
                    logger.info("ℹ️ Новых записей в KTalk пока не найдено.")
            except Exception as e:
                logger.error(f"❌ Ошибка авто-синхронизации: {e}")

            # Ждем 61 секунду, чтобы скрипт не запустился дважды в одну и ту же минуту
            await asyncio.sleep(61)
        else:
            # Если время не совпало — просто ждем 20 секунд и проверяем снова
            await asyncio.sleep(20)

async def main():
    # Запускаем две независимые фоновые задачи параллельно
    await asyncio.gather(
        bot_join_loop(),
        auto_sync_loop()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Сервис остановлен.")
