import requests
import re
import datetime
import logging

# Исправлено имя логгера на logger
logger = logging.getLogger("yandex_api")

class YandexCalendarAPI:
    def __init__(self, cookie_path="cookie.txt"):
        self.cookie_path = cookie_path
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'https://calendar.yandex.ru',
            'Referer': 'https://calendar.yandex.ru/'
        }
        self.mail_ckey = None
        self.calendar_ckey = None
        self.uid = None
        self.timezone = "Europe/Moscow"
        self.reload_session()

    def reload_session(self):
        # Исправлено с logger_yandex на logger
        logger.info("♻️ Перезагрузка сессии Яндекс API...")
        self.session.cookies.clear()
        self.mail_ckey = None
        self.calendar_ckey = None
        self._load_cookies()

    def _load_cookies(self):
        try:
            with open(self.cookie_path, 'r', encoding='utf-8') as f:
                cookie_str = f.read().strip()
            cookies = {}
            for item in cookie_str.split(';'):
                if '=' in item:
                    key, value = item.strip().split('=', 1)
                    cookies[key] = value
            self.session.cookies.update(cookies)
            self.uid = cookies.get('yandexuid')
        except FileNotFoundError:
            # Исправлено с logger_yandex на logger
            logger.error(f"❌ Файл {self.cookie_path} не найден.")

    def _get_calendar_ckey(self):
        if self.calendar_ckey: return True
        try:
            url = f"https://calendar.yandex.ru/?uid={self.uid}"
            response = self.session.get(url, headers=self.headers)
            match = re.search(r'"ckey"\s*:\s*"([^"]+)"', response.text)
            if match:
                self.calendar_ckey = match.group(1)
                return True
        except Exception:
            pass
        return False

    def get_detailed_events_for_range(self, email, start_date: datetime.date, end_date: datetime.date):
        for attempt in range(2):
            try:
                if not self._get_calendar_ckey():
                    if attempt == 0:
                        self.reload_session()
                        continue
                    return []
                
                start_str = start_date.strftime('%Y-%m-%d')
                end_str = end_date.strftime('%Y-%m-%d')
                url = "https://calendar.yandex.ru/api/models?_models=get-events-by-login"
                cid = f"MAYA-{int(datetime.datetime.now().timestamp() * 1000)}"
                
                payload = {
                    "models":[{
                        "name": "get-events-by-login",
                        "params": {
                            "limitAttendees": True,
                            "login": email,
                            "opaqueOnly": False,
                            "email": email, 
                            "from": start_str,
                            "to": end_str
                        }
                    }]
                }
                
                current_headers = self.headers.copy()
                current_headers.update({'x-yandex-maya-ckey': self.calendar_ckey, 'x-yandex-maya-uid': self.uid, 'x-yandex-maya-cid': cid, 'x-yandex-maya-timezone': self.timezone})
                resp = self.session.post(url, headers=current_headers, json=payload)
                resp_json = resp.json()
                
                if not isinstance(resp_json, dict): continue
                models = resp_json.get('models', [])
                if not models or not isinstance(models[0], dict): continue
                model = models[0]
                if model.get('status') == 'error':
                    if attempt == 0:
                        self.reload_session()
                        continue
                    return []

                data = model.get('data', {})
                if not isinstance(data, dict): continue
                events_raw = data.get('events', [])
                detailed_events = []
                msk_zone = datetime.timezone(datetime.timedelta(hours=3))

                if isinstance(events_raw, list):
                    for e in events_raw:
                        if not isinstance(e, dict): continue
                        if e.get('decision') == 'no' or e.get('availability') == 'free': continue
                        start_str_val = e.get('start')
                        end_str_val = e.get('end')
                        if not start_str_val or not end_str_val: continue
                            
                        start_iso = start_str_val.replace('Z', '+00:00')
                        end_iso = end_str_val.replace('Z', '+00:00')
                        start_dt = datetime.datetime.fromisoformat(start_iso).astimezone(msk_zone)
                        end_dt = datetime.datetime.fromisoformat(end_iso).astimezone(msk_zone)
                        name = e.get('name')
                        if isinstance(name, str): name = name.strip()
                        if not name: name = "Занят"
                            
                        detailed_events.append({
                            'start': start_dt,
                            'end': end_dt,
                            'name': name,
                            'event_id': e.get('id'),
                            'instance_start_ts': e.get('instanceStartTs'),
                            'total_attendees': e.get('totalAttendees', 0)
                        })
                return detailed_events
            except Exception as e:
                # Исправлено с logger_yandex на logger
                logger.error(f"Error in get_detailed_events: {e}")
                if attempt == 0: self.reload_session()
        return []

    def get_event_details_by_id(self, event_id, instance_start_ts=None):
        if not event_id: return "", []
        for attempt in range(2):
            try:
                if not self._get_calendar_ckey():
                    if attempt == 0:
                        self.reload_session()
                        continue
                    return "", []

                url = "https://calendar.yandex.ru/api/models?_models=get-event"
                cid = f"MAYA-{int(datetime.datetime.now().timestamp() * 1000)}"
                payload = {
                    "models": [{
                        "name": "get-event",
                        "params": {
                            "eventId": event_id,
                            "layerId": None,
                            "instanceStartTs": instance_start_ts,
                            "recurrenceAsOccurrence": False,
                            "tz": "Europe/Moscow"
                        }
                    }]
                }

                current_headers = self.headers.copy()
                current_headers.update({'x-yandex-maya-ckey': self.calendar_ckey, 'x-yandex-maya-uid': self.uid, 'x-yandex-maya-cid': cid, 'x-yandex-maya-timezone': self.timezone})
                resp = self.session.post(url, headers=current_headers, json=payload)
                data = resp.json().get('models', [{}])[0].get('data', {})
                
                desc = str(data.get('description') or "").strip()
                
                emails = [e.lower() for e in (data.get('attendees') or []) if isinstance(e, str) and "@" in e]
                if isinstance(data.get('attendees'), dict):
                    emails = [k.lower() for k in data['attendees'].keys() if "@" in k]
                elif isinstance(data.get('attendees'), list):
                    emails = [att.get('email', '').lower() for att in data['attendees'] if isinstance(att, dict) and att.get('email')]
                
                return desc, list(set(emails))
            except Exception as e:
                # Исправлено с logger_yandex на logger
                logger.error(f"Error in get_event_details: {e}")
                if attempt == 0: self.reload_session()
        return "", []