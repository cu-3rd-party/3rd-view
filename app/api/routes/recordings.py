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
                cur.execute("SELECT event_id, start_time, event_name FROM calendar_events")
                db_events_raw = cur.fetchall()

            parsed_db_events = [
                {
                    "event_id": event["event_id"],
                    "start_dt": datetime.datetime.fromisoformat(event["start_time"].replace("Z", "+00:00")),
                    "name": event["event_name"].lower(),
                }
                for event in db_events_raw
            ]

            matched_count = 0
            msk_tz = datetime.timezone(datetime.timedelta(hours=3))
            with conn.cursor() as cur:
                yield emit("progress", "🧠 Анализ совпадений (название/комната + день недели + время)...")
                for recording in recordings:
                    raw_date = recording["start_time"].replace("Z", "")
                    clean_date = raw_date.split(".")[0] + "+00:00"
                    try:
                        rec_dt = datetime.datetime.fromisoformat(clean_date).astimezone(msk_tz)
                    except Exception:
                        continue
                    rec_title = f"{recording.get('title', '')} {recording.get('room_name', '')}".lower()
                    rec_date_str = rec_dt.strftime("%Y-%m-%d")
                    for db_event in parsed_db_events:
                        if db_event["name"] in rec_title or rec_title in db_event["name"]:
                            if rec_dt.weekday() != db_event["start_dt"].weekday():
                                continue
                            rec_hours = rec_dt.hour + rec_dt.minute / 60.0
                            event_hours = db_event["start_dt"].hour + db_event["start_dt"].minute / 60.0
                            if abs(rec_hours - event_hours) <= 2.0:
                                fake_ts = f"ktalk_{recording['recording_id']}"
                                cur.execute(
                                    """
                                    INSERT INTO event_recordings
                                    (yandex_event_id, yandex_instance_start_ts, ktalk_id, recording_url, transcription_url, is_manual, recording_date)
                                    VALUES (%s, %s, %s, %s, '', FALSE, %s)
                                    ON CONFLICT (yandex_event_id, yandex_instance_start_ts) DO UPDATE SET
                                    recording_url = excluded.recording_url
                                    """,
                                    (
                                        db_event["event_id"],
                                        fake_ts,
                                        recording["recording_id"],
                                        recording["recording_url"],
                                        rec_date_str,
                                    ),
                                )
                                matched_count += 1
                                break
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
