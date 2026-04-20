import logging
from typing import Any

from app.integrations.ktalk_bot import _load_auth_data, create_client


logger = logging.getLogger("ktalk_api")


class KTalkAPI:
    def __init__(self, auth_file_path: str = "ktalk_auth.txt", auth_value: str | None = None):
        self.auth_file_path = auth_file_path
        self.auth_value = auth_value
        self.client = self._create_history_client()

    def _create_history_client(self):
        cookie_header, _session_token = _load_auth_data(self.auth_file_path, auth_value=self.auth_value)
        if not cookie_header:
            raise ValueError(
                "ktalk-bot history parsing requires ktalk_auth.txt to contain the full KTalk Cookie header, not Authorization: Session ..."
            )
        logger.info("Creating ktalk-bot history client")
        return create_client(auth_file=self.auth_file_path, auth_value=self.auth_value)

    def get_history_batch(self, skip: int = 0, top: int = 25) -> list[dict[str, Any]]:
        page = skip // top + 1
        try:
            history = self.client.get_history(max_pages=page, page_size=top)
        except Exception:
            logger.exception("Failed to fetch KTalk history batch via ktalk-bot")
            return []
        return history[skip : skip + top]

    def get_all_history_records(self, max_pages: int = 10) -> list[dict[str, Any]]:
        all_conferences: list[dict[str, Any]] = []
        skip = 0
        top = 25
        pages_fetched = 0
        while pages_fetched < max_pages:
            batch = self.get_history_batch(skip=skip, top=top)
            if not batch:
                break
            all_conferences.extend(self._parse_conference_history(conf) for conf in batch)
            if len(batch) < top:
                break
            skip += top
            pages_fetched += 1
        return all_conferences

    def _parse_conference_history(self, raw: dict[str, Any]) -> dict[str, Any]:
        return {
            "key": raw.get("key"),
            "room_name": raw.get("room_name"),
            "title": raw.get("title") or "Без названия",
            "start_time": raw.get("start_time"),
            "end_time": raw.get("end_time"),
            "participants_count": raw.get("participants_count", 0),
            "participants": raw.get("participants", []),
            "has_recording": bool(raw.get("recording_id")),
            "recording_id": raw.get("recording_id"),
            "recording_url": raw.get("recording_url"),
        }
