import datetime

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from psycopg2.extras import RealDictCursor

from app.auth import verify_admin, get_current_user, verify_user_or_admin
from app.core.config import get_settings
from app.db import db_connection
from app.integrations.ktalk_api import KTalkAPI
from app.schemas import ManualRecordingModel, SuggestRecordingModel
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

            unmatched_table_exists = False
            with conn.cursor() as cur:
                try:
                    cur.execute("SELECT 1 FROM unmatched_recordings LIMIT 1")
                    unmatched_table_exists = True
                except Exception:
                    conn.rollback()

            matched_count = 0
            with conn.cursor() as cur:
                yield emit("progress", "🧠 Анализ совпадений (поиск идеальной пары)...")
                for recording in recordings:
                    try:
                        ts_str = recording["start_time"]
                        if ts_str.endswith("Z"):
                            ts_str = ts_str.replace("Z", "+00:00")
                        rec_dt = datetime.datetime.fromisoformat(ts_str).astimezone(msk_tz)
                    except Exception:
                        continue
                                        
                    rec_title = f"{recording.get('title', '')} {recording.get('room_name', '')}".lower()
                    rec_date_str = rec_dt.strftime("%Y-%m-%d")

                    best_match = None
                    min_diff_hours = 2.0 

                    for db_event in parsed_db_events:
                        if db_event["dt"].date() != rec_dt.date():
                            continue
                        
                        if db_event["name"] not in rec_title and rec_title not in db_event["name"]:
                            continue
                        
                        time_diff_hours = abs((db_event["dt"] - rec_dt).total_seconds()) / 3600.0
                        
                        if time_diff_hours < min_diff_hours:
                            min_diff_hours = time_diff_hours
                            best_match = db_event

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
                        
                        if unmatched_table_exists:
                            cur.execute("DELETE FROM unmatched_recordings WHERE ktalk_id = %s", (recording["recording_id"],))
                    else:
                        if unmatched_table_exists:
                            cur.execute(
                                """
                                INSERT INTO unmatched_recordings (ktalk_id, title, start_time, recording_url, room_name)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (ktalk_id) DO UPDATE SET
                                title = EXCLUDED.title,
                                room_name = EXCLUDED.room_name
                                """,
                                (
                                    recording["recording_id"],
                                    recording.get("title", ""),
                                    recording.get("start_time", ""),
                                    recording["recording_url"],
                                    recording.get("room_name", "")
                                )
                            )
                            
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


@router.post("/api/recordings/suggest")
async def suggest_recording(data: SuggestRecordingModel, user_email: str = Depends(get_current_user)) -> dict[str, str]:
    if not data.suggested_url.startswith("https://centraluniversity.ktalk.ru/recordings/"):
        return {"status": "error", "message": "Неверный формат ссылки. Нужна ссылка с ktalk."}

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO suggested_recordings (yandex_event_id, yandex_instance_start_ts, suggested_url, suggested_by_email)
                VALUES (%s, %s, %s, %s)
                """,
                (data.yandex_event_id, data.yandex_instance_start_ts, data.suggested_url.strip(), user_email),
            )
        conn.commit()
    return {"status": "ok"}


@router.get("/api/admin/recordings/suggestions")
async def get_suggested_recordings(admin: str = Depends(verify_admin)) -> list[dict]:
    query = """
        SELECT s.id, s.yandex_event_id, s.yandex_instance_start_ts, s.suggested_url, s.suggested_by_email, s.created_at, c.event_name, c.start_time
        FROM suggested_recordings s
        LEFT JOIN calendar_events c ON s.yandex_event_id = c.event_id AND s.yandex_instance_start_ts = c.instance_start_ts
        ORDER BY s.created_at DESC
    """
    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            results = cur.fetchall()
            
    # Convert datetime to string
    for row in results:
        if row.get("created_at"):
            row["created_at"] = row["created_at"].isoformat()
            
    return results


@router.delete("/api/admin/recordings/suggestions/{suggestion_id}")
async def delete_suggested_recording(suggestion_id: int, admin: str = Depends(verify_admin)) -> dict:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM suggested_recordings WHERE id = %s", (suggestion_id,))
        conn.commit()
    return {"status": "ok"}


@router.get("/api/recordings/unmatched")
async def get_unmatched_recordings(user: dict = Depends(verify_user_or_admin)) -> list[dict]:
    query = """
        SELECT ktalk_id, title, start_time, recording_url, room_name, created_at
        FROM unmatched_recordings
        WHERE recording_url NOT IN (SELECT suggested_url FROM suggested_recordings)
        AND recording_url NOT IN (SELECT recording_url FROM event_recordings WHERE is_manual=TRUE)
        ORDER BY start_time DESC
    """
    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(query)
                results = cur.fetchall()
            except Exception:
                conn.rollback()
                results = []
            
    for row in results:
        if row.get("created_at"):
            row["created_at"] = row["created_at"].isoformat()
            
    return results
