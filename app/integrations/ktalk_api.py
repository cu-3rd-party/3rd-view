import logging
from typing import Any

import requests


logger = logging.getLogger("ktalk_api")


class KTalkAPI:
    def __init__(self, auth_file_path: str = "ktalk_auth.txt"):
        self.auth_file_path = auth_file_path
        self.session = requests.Session()
        self.base_url = "https://centraluniversity.ktalk.ru"
        self.auth_token = None
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Platform": "web",
        }
        self.reload_session()

    def reload_session(self) -> None:
        logger.info("Reloading KTalk session")
        self.session.cookies.clear()
        self.auth_token = None
        self._load_auth()

    def _load_auth(self) -> None:
        try:
            with open(self.auth_file_path, "r", encoding="utf-8") as file_obj:
                token = file_obj.read().strip()
            if token:
                self.auth_token = token
                self.session.headers.update(self.headers)
                self.session.headers.update({"Authorization": self.auth_token})
            else:
                logger.error("KTalk auth file is empty")
        except FileNotFoundError:
            logger.error("KTalk auth file %s was not found", self.auth_file_path)

    def get_history_batch(self, skip: int = 0, top: int = 25) -> list[dict[str, Any]]:
        if not self.auth_token:
            logger.error("KTalk auth token is missing")
            return []

        params = {"skip": skip, "top": top, "includeUnfinished": "true"}
        for attempt in range(2):
            try:
                response = self.session.get(f"{self.base_url}/api/conferenceshistory", params=params)
                if response.status_code in (401, 403):
                    if attempt == 0:
                        self.reload_session()
                        continue
                    return []
                response.raise_for_status()
                return response.json().get("conferences", [])
            except Exception:
                logger.exception("Failed to fetch KTalk history batch")
                if attempt == 0:
                    self.reload_session()
        return []

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
        artifacts = raw.get("artifacts", {})
        participants = []
        for participant in artifacts.get("participants", []):
            if participant.get("isAnonymous"):
                participants.append(participant.get("anonymousName", "Гость"))
            else:
                user_info = participant.get("userInfo", {})
                name = f"{user_info.get('firstname', '')} {user_info.get('surname', '')}".strip()
                if name:
                    participants.append(name)

        recording_id = None
        for content_item in artifacts.get("content", []):
            if content_item.get("type") == "record":
                recording_id = content_item.get("id")

        return {
            "key": raw.get("key"),
            "room_name": raw.get("roomName"),
            "title": raw.get("title", "Без названия"),
            "start_time": raw.get("startTime"),
            "end_time": raw.get("endTime"),
            "participants_count": raw.get("participantsCount", 0),
            "participants": participants,
            "has_recording": bool(recording_id),
            "recording_id": recording_id,
            "recording_url": f"{self.base_url}/recordings/{recording_id}" if recording_id else None,
        }
