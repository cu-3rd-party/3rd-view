# Файл: integrations/ktalk_bot.py
import asyncio
import requests
import websockets
import json
import re
import random
import string
import uuid
import os
from datetime import datetime, timezone

class KTalkBot:
    def __init__(self, auth_file="ktalk_auth.txt"):
        """
        Инициализация бота.
        Читает токен из файла и подготавливает HTTP-сессию.
        """
        self.auth_file = auth_file
        self.token = self._load_token()
        self.base_url = "https://centraluniversity.ktalk.ru"
        
        # Глобальная HTTP-сессия для сбора куки
        self.http_session = requests.Session()
        self.http_session.headers.update({
            "Authorization": f"Session {self.token}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "x-platform": "web"
        })

    def _load_token(self):
        if not os.path.exists(self.auth_file):
            raise FileNotFoundError(f"Файл {self.auth_file} не найден!")
            
        with open(self.auth_file, "r", encoding="utf-8") as f:
            content = f.read().strip()
            
        if content.startswith("Session "):
            return content.split(" ", 1)[1]
        return content

    def _gen_req_id(self):
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

    def _get_cookie_string(self):
        return "; ".join([f"{k}={v}" for k, v in self.http_session.cookies.get_dict().items()])

    def get_user_context(self):
        url = f"{self.base_url}/api/context"
        try:
            resp = self.http_session.get(url)
            if resp.status_code == 200:
                data = resp.json().get("user", {})
                return {
                    "id": data.get("id", data.get("login", "")),
                    "firstName": data.get("firstname", "Студент"),
                    "lastName": data.get("surname", ""),
                }
        except Exception as e:
            print(f"[-] Ошибка API Контекста: {e}")
        return None

    def get_room_info(self, short_name):
        url = f"{self.base_url}/api/rooms/{short_name}"
        try:
            resp = self.http_session.get(url)
            if resp.status_code == 200:
                return resp.json().get("conferenceId")
        except Exception as e:
            print(f"[-] Ошибка API Rooms: {e}")
        return None

    def send_activity(self, short_name):
        url = f"{self.base_url}/api/UserActivities"
        payload = [{
            "$type": "GotoRoom",
            "cameraEnabled": False,
            "micEnabled": False, 
            "roomName": short_name,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }]
        try:
            self.http_session.post(url, json=payload, timeout=5)
        except:
            pass

    async def _background_activity_loop(self, short_name):
        try:
            while True:
                await asyncio.sleep(600)
                await asyncio.to_thread(self.send_activity, short_name)
        except asyncio.CancelledError:
            pass 

    async def _connect_system_ws(self, short_name, user_id):
        ws_url = "wss://centraluniversity.ktalk.ru/system/ws"
        headers = {
            "Origin": self.base_url,
            "Cookie": self._get_cookie_string(),
            "User-Agent": self.http_session.headers["User-Agent"]
        }

        print("[*] 1. Подключаемся к системе KTalk (Чат и Статусы)...")
        
        try:
            async with websockets.connect(ws_url, additional_headers=headers) as ws:
                await ws.send(json.dumps({
                    "a": "connect", "reqId": self._gen_req_id(),
                    "data": {"signInToken": self.token, "clientType": "Web", "webAppVersion": "master"}
                }))
                
                auth_resp = json.loads(await ws.recv())
                session_id = auth_resp.get("data", {}).get("sessionId")
                
                if not session_id:
                    print("[-] Ошибка: не удалось получить sessionId для чата")
                    return

                await ws.send(json.dumps({"a": "message_subscribe", "reqId": self._gen_req_id(), "data": {"topic": "personal"}, "session": session_id}))
                await ws.send(json.dumps({"a": "user_status", "reqId": self._gen_req_id(), "data": {"userKey": user_id, "status": "inMeeting"}, "session": session_id}))
                await ws.send(json.dumps({"a": "chat_join", "reqId": self._gen_req_id(), "data": {"name": short_name, "popup": False, "platform": "web"}, "session": session_id}))

                while True:
                    await asyncio.sleep(20)
                    await ws.send(json.dumps({"a": "ping", "reqId": self._gen_req_id(), "data": {}, "session": session_id}))
                    await ws.send(json.dumps({"a": "chat_ping", "reqId": self._gen_req_id(), "data": {"name": short_name}, "session": session_id}))
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _connect_xmpp_ws(self, conference_id, user_data):
        ws_url = f"wss://centraluniversity.ktalk.ru/jitsi/xmpp-websocket?room={conference_id}&sessionToken={self.token}"
        headers = {
            "Origin": self.base_url,
            "Cookie": self._get_cookie_string(),
            "User-Agent": self.http_session.headers["User-Agent"],
            "Sec-WebSocket-Protocol": "xmpp"
        }

        print("[*] 2. Подключаемся к видеомосту XMPP...")
        
        try:
            async with websockets.connect(ws_url, additional_headers=headers, subprotocols=["xmpp"]) as ws:
                await ws.send('<open to="meet.jitsi" version="1.0" xmlns="urn:ietf:params:xml:ns:xmpp-framing"/>')
                state = "AUTH"
                handled_stanzas = 0
                system_nick = ''.join(random.choices("0123456789abcdef", k=8))
                
                async def xmpp_client_ping_loop():
                    try:
                        while True:
                            await asyncio.sleep(10)
                            ping_id = f"{uuid.uuid4()}:sendIQ"
                            await ws.send(f'<iq id="{ping_id}" to="meet.jitsi" type="get" xmlns="jabber:client"><ping xmlns="urn:xmpp:ping"/></iq>')
                    except asyncio.CancelledError:
                        pass

                ping_task = None

                try:
                    while True:
                        msg = await ws.recv()
                        if isinstance(msg, bytes): msg = msg.decode('utf-8', errors='ignore')
                        
                        if msg.startswith("<iq") or msg.startswith("<presence") or msg.startswith("<message"):
                            handled_stanzas += 1
                        
                        if state == "AUTH" and "ANONYMOUS" in msg:
                            await ws.send('<auth mechanism="ANONYMOUS" xmlns="urn:ietf:params:xml:ns:xmpp-sasl"/>')
                            state = "WAIT_SUCCESS"
                        
                        elif state == "WAIT_SUCCESS" and "success" in msg:
                            await ws.send('<open to="meet.jitsi" version="1.0" xmlns="urn:ietf:params:xml:ns:xmpp-framing"/>')
                            state = "BIND"
                        
                        elif state == "BIND" and "urn:ietf:params:xml:ns:xmpp-bind" in msg:
                            await ws.send('<iq type="set" id="bind_1" xmlns="jabber:client"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"/></iq>')
                            state = "ENABLE_SM"
                        
                        elif state == "ENABLE_SM" and "bind_1" in msg:
                            await ws.send('<enable xmlns="urn:xmpp:sm:3" resume="false"/>')
                            
                            user_info = {
                                "key": user_data["id"], 
                                "firstName": user_data["firstName"],
                                "lastName": user_data["lastName"],
                                "middleName": "",
                                "isKiosk": False
                            }
                            escaped_ui = json.dumps(user_info, ensure_ascii=False).replace('"', '&quot;')
                            
                            source_info = {
                                f"{system_nick}-a0": {"muted": True}, 
                                f"{system_nick}-v0": {"muted": True}
                            }
                            escaped_si = json.dumps(source_info, separators=(',', ':')).replace('"', '&quot;')

                            presence_msg = (
                                f'<presence to="{conference_id}@muc.meet.jitsi/{system_nick}" xmlns="jabber:client">'
                                f'<x xmlns="http://jabber.org/protocol/muc"/>' 
                                f'<audiomuted>true</audiomuted>'
                                f'<videomuted>true</videomuted>'
                                f'<stats-id>Crawford-aBY</stats-id>'
                                f'<c hash="sha-1" node="https://jitsi.org/jitsi-meet" ver="+mpajJhafj8jFogLBKsPbQfMgzU=" xmlns="http://jabber.org/protocol/caps"/>'
                                f'<jitsi_participant_codecList>vp9,vp8,h264,av1</jitsi_participant_codecList>'
                                f'<nick xmlns="http://jabber.org/protocol/nick">{user_data["firstName"]}</nick>'
                                f'<jitsi_participant_user-info>{escaped_ui}</jitsi_participant_user-info>'
                                f'<SourceInfo>{escaped_si}</SourceInfo>'
                                f'<jitsi_participant_video-size>{{&quot;width&quot;:960,&quot;height&quot;:720}}</jitsi_participant_video-size>'
                                f'</presence>'
                            )
                            await ws.send(presence_msg)
                            ping_task = asyncio.create_task(xmpp_client_ping_loop())
                            state = "JOINED"

                        if state == "JOINED":
                            if f"/{system_nick}" in msg and "status code='110'" in msg:
                                print(f"\n[+++] УСПЕХ! Вы в сетке конференции! Имя: {user_data['firstName']}\n")

                        if "<r xmlns" in msg:
                            await ws.send(f"<a xmlns='urn:xmpp:sm:3' h='{handled_stanzas}'/>")

                        if "urn:xmpp:ping" in msg and "get" in msg and "<ping" in msg:
                            match = re.search(r'id=["\']([^"\']+)["\']', msg)
                            if match:
                                await ws.send(f'<iq type="result" id="{match.group(1)}" xmlns="jabber:client"/>')

                        if state == "JOINED" and "<iq" in msg and ("type='set'" in msg or 'type="set"' in msg):
                            match_id = re.search(r'id=["\']([^"\']+)["\']', msg)
                            match_from = re.search(r'from=["\']([^"\']+)["\']', msg)
                            if match_id and match_from and "focus" in match_from.group(1):
                                iq_id = match_id.group(1)
                                focus_jid = match_from.group(1)
                                await ws.send(f'<iq type="result" id="{iq_id}" to="{focus_jid}" xmlns="jabber:client"/>')

                finally:
                    if ping_task: ping_task.cancel()
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _run_bot(self, link, time_exit):
        """Внутренний метод для запуска и удержания бота указанное время"""
        user_data = self.get_user_context()
        if not user_data:
            print("[-] Ошибка авторизации. Проверь ktalk_auth.txt.")
            return

        short_name = link.split("/")[-1]
        real_conference_id = self.get_room_info(short_name)
        
        if not real_conference_id:
            print("[-] Не удалось получить ID комнаты. Сервер отклонил запрос.")
            return

        print("="*50)
        print(f"[*] Студент: {user_data['firstName']} {user_data['lastName']}")
        print(f"[*] Комната: {short_name}")
        print(f"[*] Таймер выхода: {time_exit} сек.")
        print("="*50)

        self.send_activity(short_name)
        
        tasks = [
            asyncio.create_task(self._background_activity_loop(short_name)),
            asyncio.create_task(self._connect_system_ws(short_name, user_data["id"])),
            asyncio.create_task(self._connect_xmpp_ws(real_conference_id, user_data))
        ]
        
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=time_exit)
        except asyncio.TimeoutError:
            print(f"\n[!] Время вышло ({time_exit} сек). Завершаем работу.")
        finally:
            print("[*] Лекция покинута.")


def connect(link: str, time_exit: int = 5, auth_file: str = "ktalk_auth.txt"):
    bot = KTalkBot(auth_file=auth_file)
    asyncio.run(bot._run_bot(link, time_exit))