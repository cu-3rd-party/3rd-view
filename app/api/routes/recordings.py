import datetime

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from psycopg2.extras import RealDictCursor

from app.auth import verify_admin
from app.core.config import get_settings
from app.db import db_connection
from app.integrations.ktalk_api import KTalkAPI
from app.schemas import ManualRecordingModel
from app.services.common import emit


router = APIRouter()


async def sync_recordings_generator():
    settings = get_settings()
    try:
        yield emit("progress", "🔌 Подключение к KTalk API...")
        ktalk = KTalkAPI(auth_file_path=str(settings.ktalk_auth_file))
        conferences = ktalk.get_all_history_records(max_pages=10)
        recordings = [conference for conference in conferences if conference.get("has_recording") and conference.get("recording_url")]
        yield emit("progress", f"📥 Загружено {len(conferences)} конференций, из них {len(recordings)} с записями.")

        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Берем и instance_start_ts (для БД), и start_time (для вычислений)
                cur.execute("SELECT event_id, instance_start_ts, start_time, event_name FROM calendar_events")
                db_events_raw = cur.fetchall()

            parsed_db_events = []
            msk_tz = datetime.timezone(datetime.timedelta(hours=3))
            
            for event in db_events_raw:
                try:
                    # ИСПРАВЛЕНИЕ 1: Для математики берем ИМЕННО start_time!
                    yandex_dt = datetime.datetime.fromisoformat(event["start_time"].replace("Z", "+00:00")).astimezone(msk_tz)
                    
                    parsed_db_events.append({
                        "event_id": event["event_id"],
                        "instance_start_ts": event["instance_start_ts"],
                        "dt": yandex_dt,
                        "name": event["event_name"].lower()
                    })
                except Exception:
                    continue

            matched_count = 0
            with conn.cursor() as cur:
                yield emit("progress", "🧠 Анализ совпадений (поиск идеальной пары)...")
                for recording in recordings:
                    try:
                        rec_dt_str = recording["start_time"].replace("Z", "").split(".")[0] + "+00:00"
                        rec_dt = datetime.datetime.fromisoformat(rec_dt_str).astimezone(msk_tz)
                    except Exception:
                        continue
                    
                    rec_title = f"{recording.get('title', '')} {recording.get('room_name', '')}".lower()
                    rec_date_str = rec_dt.strftime("%Y-%m-%d")

                    # ИСПРАВЛЕНИЕ 2: Ищем наилучшее совпадение по времени
                    best_match = None
                    min_diff_hours = 2.0  # Максимальное окно - 2 часа

                    for db_event in parsed_db_events:
                        if db_event["dt"].date() != rec_dt.date():
                            continue
                        
                        if db_event["name"] not in rec_title and rec_title not in db_event["name"]:
                            continue
                        
                        time_diff_hours = abs((db_event["dt"] - rec_dt).total_seconds()) / 3600.0
                        
                        # Если эта пара ближе по времени, запоминаем её
                        if time_diff_hours < min_diff_hours:
                            min_diff_hours = time_diff_hours
                            best_match = db_event

                    # Если нашли идеальное совпадение - записываем
                    if best_match:
                        cur.execute(
                            """
                            INSERT INTO event_recordings
                            (yandex_event_id, yandex_instance_start_ts, ktalk_id, recording_url, transcription_url, is_manual, recording_date)
                            VALUES (%s, %s, %s, %s, '', FALSE, %s)
                            ON CONFLICT (yandex_event_id, yandex_instance_start_ts) DO UPDATE SET
                            recording_url = excluded.recording_url,
                            ktalk_id = excluded.ktalk_id
                            """,
                            (
                                best_match["event_id"],
                                best_match["instance_start_ts"], 
                                recording["recording_id"],
                                recording["recording_url"],
                                rec_date_str,
                            ),
                        )
                        matched_count += 1
            conn.commit()
        yield emit("done", {"count": matched_count})
    except Exception as exc:
        yield emit("error", f"Ошибка: {exc}")


@router.post("/api/recordings/sync")
async def sync_recordings_endpoint(admin: str = Depends(verify_admin)) -> StreamingResponse:
    return StreamingResponse(sync_recordings_generator(), media_type="text/plain")


@router.post("/api/recordings/manual")
async def set_manual_recording(data: ManualRecordingModel, admin: str = Depends(verify_admin)) -> dict[str, str]:
    fake_ts = f"manual_{data.recording_date}"
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO event_recordings
                (yandex_event_id, yandex_instance_start_ts, recording_url, is_manual, recording_date)
                VALUES (%s, %s, %s, TRUE, %s)
                ON CONFLICT (yandex_event_id, yandex_instance_start_ts) DO UPDATE SET
                recording_url = excluded.recording_url,
                is_manual = TRUE
                """,
                (data.yandex_event_id, fake_ts, data.recording_url.strip(), data.recording_date),
            )
        conn.commit()
    return {"status": "ok"}
