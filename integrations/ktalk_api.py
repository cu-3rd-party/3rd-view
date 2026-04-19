# Файл: integrations/ktalk_api.py
import requests
import logging
from typing import List, Dict, Any

# Настройка логгера
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ktalk_api")

class KTalkAPI:
    def __init__(self, auth_file_path="ktalk_auth.txt"):
        """
        Инициализация клиента для API KTalk.
        :param auth_file_path: путь к файлу, в котором лежит токен (например, "Session VOhszguFTRq9i15vzpSf")
        """
        self.auth_file_path = auth_file_path
        self.session = requests.Session()
        self.base_url = "https://centraluniversity.ktalk.ru"
        self.auth_token = None
        
        # Базовые заголовки
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-Platform': 'web'
        }
        
        self.reload_session()

    def reload_session(self):
        """Перезагрузка сессии и обновление токена из файла"""
        logger.info("♻️ Перезагрузка сессии KTalk API...")
        self.session.cookies.clear()
        self.auth_token = None
        self._load_auth()

    def _load_auth(self):
        """Загрузка заголовка Authorization из файла"""
        try:
            with open(self.auth_file_path, 'r', encoding='utf-8') as f:
                token = f.read().strip()
                
            if token:
                self.auth_token = token
                self.session.headers.update(self.headers)
                self.session.headers.update({'Authorization': self.auth_token})
                logger.info("✅ Токен авторизации KTalk успешно загружен.")
            else:
                logger.error("❌ Файл авторизации пуст.")
        except FileNotFoundError:
            logger.error(f"❌ Файл {self.auth_file_path} не найден. Создайте его и поместите туда токен 'Session ...'")

    def get_history_batch(self, skip: int = 0, top: int = 25) -> List[Dict[str, Any]]:
        """
        Получает одну страницу ИСТОРИИ КОНФЕРЕНЦИЙ.
        """
        if not self.auth_token:
            logger.error("❌ Нет токена авторизации. Запрос отменен.")
            return []

        url = f"{self.base_url}/api/conferenceshistory"
        params = {
            "skip": skip,
            "top": top,
            "includeUnfinished": "true"
        }

        for attempt in range(2):
            try:
                response = self.session.get(url, params=params)
                
                if response.status_code in [401, 403]:
                    logger.warning(f"⚠️ Ошибка доступа ({response.status_code}). Протух токен?")
                    if attempt == 0:
                        self.reload_session()
                        continue
                    return []

                response.raise_for_status()
                data = response.json()
                
                return data.get("conferences", [])

            except Exception as e:
                logger.error(f"❌ Ошибка в get_history_batch: {e}")
                if attempt == 0: 
                    self.reload_session()
        return []

    def get_all_history_records(self, max_pages: int = 10) -> List[Dict[str, Any]]:
        """
        Вытягивает историю конференций, перебирая страницы.
        """
        all_conferences = []
        skip = 0
        top = 25
        pages_fetched = 0

        logger.info("🚀 Начинаю сбор ИСТОРИИ КОНФЕРЕНЦИЙ из KTalk...")

        while pages_fetched < max_pages:
            batch = self.get_history_batch(skip=skip, top=top)
            
            if not batch:
                break
                
            parsed_batch = [self._parse_conference_history(conf) for conf in batch]
            all_conferences.extend(parsed_batch)
            
            if len(batch) < top:
                break
                
            skip += top
            pages_fetched += 1

        logger.info(f"✅ Успешно собрано {len(all_conferences)} конференций.")
        return all_conferences

    def _parse_conference_history(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Парсит структуру из /api/conferenceshistory
        """
        artifacts = raw.get("artifacts", {})
        participants = []
        for p in artifacts.get("participants", []):
            if p.get("isAnonymous"):
                participants.append(p.get("anonymousName", "Гость"))
            else:
                u_info = p.get("userInfo", {})
                name = f"{u_info.get('firstname', '')} {u_info.get('surname', '')}".strip()
                if name:
                    participants.append(name)

        recording_id = None
        for content_item in artifacts.get("content", []):
            if content_item.get("type") == "record":
                recording_id = content_item.get("id")

        recording_url = f"{self.base_url}/recordings/{recording_id}" if recording_id else None

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
            "recording_url": recording_url
        }