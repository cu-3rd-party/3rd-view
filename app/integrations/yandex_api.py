import datetime
import logging
import re

import requests


logger = logging.getLogger("yandex_api")


class YandexCalendarAPI:
    def __init__(self, cookie_path: str = "cookie.txt"):
        self.cookie_path = cookie_path
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://calendar.yandex.ru",
            "Referer": "https://calendar.yandex.ru/",
        }
        self.calendar_ckey = None
        self.uid = None
        self.timezone = "Europe/Moscow"
        self.reload_session()

    def reload_session(self) -> None:
        logger.info("Reloading Yandex Calendar session")
        self.session.cookies.clear()
        self.calendar_ckey = None
        self._load_cookies()

    def _load_cookies(self) -> None:
        try:
            with open(self.cookie_path, "r", encoding="utf-8") as file_obj:
                cookie_str = file_obj.read().strip()
            cookies = {}
            for item in cookie_str.split(";"):
                if "=" in item:
                    key, value = item.strip().split("=", 1)
                    cookies[key] = value
            self.session.cookies.update(cookies)
            self.uid = cookies.get("yandexuid")
        except FileNotFoundError:
            logger.error("Cookie file %s was not found", self.cookie_path)

    def _get_calendar_ckey(self) -> bool:
        if self.calendar_ckey:
            return True
        try:
            response = self.session.get(f"https://calendar.yandex.ru/?uid={self.uid}", headers=self.headers)
            match = re.search(r'"ckey"\s*:\s*"([^"]+)"', response.text)
            if match:
                self.calendar_ckey = match.group(1)
                return True
        except Exception:
            logger.exception("Failed to resolve Yandex calendar ckey")
        return False

    def get_detailed_events_for_range(
        self,
        email: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list[dict]:
        for attempt in range(2):
            try:
                if not self._get_calendar_ckey():
                    if attempt == 0:
                        self.reload_session()
                        continue
                    return []

                payload = {
                    "models": [
                        {
                            "name": "get-events-by-login",
                            "params": {
                                "limitAttendees": True,
                                "login": email,
                                "opaqueOnly": False,
                                "email": email,
                                "from": start_date.strftime("%Y-%m-%d"),
                                "to": end_date.strftime("%Y-%m-%d"),
                            },
                        }
                    ]
                }
                current_headers = self.headers.copy()
                current_headers.update(
                    {
                        "x-yandex-maya-ckey": self.calendar_ckey,
                        "x-yandex-maya-uid": self.uid,
                        "x-yandex-maya-cid": f"MAYA-{int(datetime.datetime.now().timestamp() * 1000)}",
                        "x-yandex-maya-timezone": self.timezone,
                    }
                )
                response = self.session.post(
                    "https://calendar.yandex.ru/api/models?_models=get-events-by-login",
                    headers=current_headers,
                    json=payload,
                )
                model = response.json().get("models", [{}])[0]
                if model.get("status") == "error":
                    if attempt == 0:
                        self.reload_session()
                        continue
                    return []

                events_raw = model.get("data", {}).get("events", [])
                detailed_events = []
                msk_zone = datetime.timezone(datetime.timedelta(hours=3))
                for event in events_raw:
                    if not isinstance(event, dict):
                        continue
                    if event.get("decision") == "no" or event.get("availability") == "free":
                        continue
                    start_value = event.get("start")
                    end_value = event.get("end")
                    if not start_value or not end_value:
                        continue
                    detailed_events.append(
                        {
                            "start": datetime.datetime.fromisoformat(start_value.replace("Z", "+00:00")).astimezone(msk_zone),
                            "end": datetime.datetime.fromisoformat(end_value.replace("Z", "+00:00")).astimezone(msk_zone),
                            "name": (event.get("name") or "Занят").strip() or "Занят",
                            "event_id": event.get("id"),
                            "instance_start_ts": event.get("instanceStartTs"),
                            "total_attendees": event.get("totalAttendees", 0),
                        }
                    )
                return detailed_events
            except Exception:
                logger.exception("Failed to fetch Yandex calendar events")
                if attempt == 0:
                    self.reload_session()
        return []

    def get_event_details_by_id(self, event_id: str, instance_start_ts: str | None = None) -> tuple[str, list[str]]:
        if not event_id:
            return "", []

        for attempt in range(2):
            try:
                if not self._get_calendar_ckey():
                    if attempt == 0:
                        self.reload_session()
                        continue
                    return "", []

                payload = {
                    "models": [
                        {
                            "name": "get-event",
                            "params": {
                                "eventId": event_id,
                                "layerId": None,
                                "instanceStartTs": instance_start_ts,
                                "recurrenceAsOccurrence": False,
                                "tz": "Europe/Moscow",
                            },
                        }
                    ]
                }
                current_headers = self.headers.copy()
                current_headers.update(
                    {
                        "x-yandex-maya-ckey": self.calendar_ckey,
                        "x-yandex-maya-uid": self.uid,
                        "x-yandex-maya-cid": f"MAYA-{int(datetime.datetime.now().timestamp() * 1000)}",
                        "x-yandex-maya-timezone": self.timezone,
                    }
                )
                response = self.session.post(
                    "https://calendar.yandex.ru/api/models?_models=get-event",
                    headers=current_headers,
                    json=payload,
                )
                data = response.json().get("models", [{}])[0].get("data", {})
                attendees = data.get("attendees")
                emails = [email.lower() for email in attendees or [] if isinstance(email, str) and "@" in email]
                if isinstance(attendees, dict):
                    emails = [key.lower() for key in attendees if "@" in key]
                elif isinstance(attendees, list):
                    emails = [
                        attendee.get("email", "").lower()
                        for attendee in attendees
                        if isinstance(attendee, dict) and attendee.get("email")
                    ]
                return str(data.get("description") or "").strip(), list(set(emails))
            except Exception:
                logger.exception("Failed to fetch Yandex event details")
                if attempt == 0:
                    self.reload_session()
        return "", []
