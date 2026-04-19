import asyncio
import json
import os
import random
import re
import string
import uuid
from datetime import datetime, timezone

import requests
import websockets


class KTalkBot:
    def __init__(self, auth_file: str = "ktalk_auth.txt"):
        self.auth_file = auth_file
        self.token = self._load_token()
        self.base_url = "https://centraluniversity.ktalk.ru"
        self.http_session = requests.Session()
        self.http_session.headers.update(
            {
                "Authorization": f"Session {self.token}",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "x-platform": "web",
            }
        )

    def _load_token(self) -> str:
        if not os.path.exists(self.auth_file):
            raise FileNotFoundError(f"Файл {self.auth_file} не найден")
        with open(self.auth_file, "r", encoding="utf-8") as file_obj:
            content = file_obj.read().strip()
        return content.split(" ", 1)[1] if content.startswith("Session ") else content

    def _gen_req_id(self) -> str:
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=10))

    def _get_cookie_string(self) -> str:
        return "; ".join(f"{key}={value}" for key, value in self.http_session.cookies.get_dict().items())

    def get_user_context(self) -> dict | None:
        try:
            response = self.http_session.get(f"{self.base_url}/api/context")
            if response.status_code == 200:
                user = response.json().get("user", {})
                return {
                    "id": user.get("id", user.get("login", "")),
                    "firstName": user.get("firstname", "Студент"),
                    "lastName": user.get("surname", ""),
                }
        except Exception:
            return None
        return None

    def get_room_info(self, short_name: str) -> str | None:
        try:
            response = self.http_session.get(f"{self.base_url}/api/rooms/{short_name}")
            if response.status_code == 200:
                return response.json().get("conferenceId")
        except Exception:
            return None
        return None

    def send_activity(self, short_name: str) -> None:
        payload = [
            {
                "$type": "GotoRoom",
                "cameraEnabled": False,
                "micEnabled": False,
                "roomName": short_name,
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }
        ]
        try:
            self.http_session.post(f"{self.base_url}/api/UserActivities", json=payload, timeout=5)
        except Exception:
            pass

    async def _background_activity_loop(self, short_name: str) -> None:
        try:
            while True:
                await asyncio.sleep(600)
                await asyncio.to_thread(self.send_activity, short_name)
        except asyncio.CancelledError:
            pass

    async def _connect_system_ws(self, short_name: str, user_id: str) -> None:
        headers = {
            "Origin": self.base_url,
            "Cookie": self._get_cookie_string(),
            "User-Agent": self.http_session.headers["User-Agent"],
        }
        try:
            async with websockets.connect("wss://centraluniversity.ktalk.ru/system/ws", additional_headers=headers) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "a": "connect",
                            "reqId": self._gen_req_id(),
                            "data": {
                                "signInToken": self.token,
                                "clientType": "Web",
                                "webAppVersion": "master",
                            },
                        }
                    )
                )
                auth_resp = json.loads(await ws.recv())
                session_id = auth_resp.get("data", {}).get("sessionId")
                if not session_id:
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

    async def _connect_xmpp_ws(self, conference_id: str, user_data: dict) -> None:
        headers = {
            "Origin": self.base_url,
            "Cookie": self._get_cookie_string(),
            "User-Agent": self.http_session.headers["User-Agent"],
            "Sec-WebSocket-Protocol": "xmpp",
        }
        try:
            async with websockets.connect(
                f"wss://centraluniversity.ktalk.ru/jitsi/xmpp-websocket?room={conference_id}&sessionToken={self.token}",
                additional_headers=headers,
                subprotocols=["xmpp"],
            ) as ws:
                await ws.send('<open to="meet.jitsi" version="1.0" xmlns="urn:ietf:params:xml:ns:xmpp-framing"/>')
                state = "AUTH"
                handled_stanzas = 0
                system_nick = "".join(random.choices("0123456789abcdef", k=8))

                async def xmpp_client_ping_loop() -> None:
                    try:
                        while True:
                            await asyncio.sleep(10)
                            await ws.send(
                                f'<iq id="{uuid.uuid4()}:sendIQ" to="meet.jitsi" type="get" xmlns="jabber:client"><ping xmlns="urn:xmpp:ping"/></iq>'
                            )
                    except asyncio.CancelledError:
                        pass

                ping_task = None
                try:
                    while True:
                        message = await ws.recv()
                        if isinstance(message, bytes):
                            message = message.decode("utf-8", errors="ignore")
                        if message.startswith("<iq") or message.startswith("<presence") or message.startswith("<message"):
                            handled_stanzas += 1
                        if state == "AUTH" and "ANONYMOUS" in message:
                            await ws.send('<auth mechanism="ANONYMOUS" xmlns="urn:ietf:params:xml:ns:xmpp-sasl"/>')
                            state = "WAIT_SUCCESS"
                        elif state == "WAIT_SUCCESS" and "success" in message:
                            await ws.send('<open to="meet.jitsi" version="1.0" xmlns="urn:ietf:params:xml:ns:xmpp-framing"/>')
                            state = "BIND"
                        elif state == "BIND" and "urn:ietf:params:xml:ns:xmpp-bind" in message:
                            await ws.send('<iq type="set" id="bind_1" xmlns="jabber:client"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"/></iq>')
                            state = "ENABLE_SM"
                        elif state == "ENABLE_SM" and "bind_1" in message:
                            await ws.send('<enable xmlns="urn:xmpp:sm:3" resume="false"/>')
                            escaped_ui = json.dumps(
                                {
                                    "key": user_data["id"],
                                    "firstName": user_data["firstName"],
                                    "lastName": user_data["lastName"],
                                    "middleName": "",
                                    "isKiosk": False,
                                },
                                ensure_ascii=False,
                            ).replace('"', "&quot;")
                            escaped_si = json.dumps(
                                {f"{system_nick}-a0": {"muted": True}, f"{system_nick}-v0": {"muted": True}},
                                separators=(",", ":"),
                            ).replace('"', "&quot;")
                            await ws.send(
                                f'<presence to="{conference_id}@muc.meet.jitsi/{system_nick}" xmlns="jabber:client">'
                                f'<x xmlns="http://jabber.org/protocol/muc"/>'
                                f"<audiomuted>true</audiomuted>"
                                f"<videomuted>true</videomuted>"
                                f'<stats-id>Crawford-aBY</stats-id>'
                                f'<c hash="sha-1" node="https://jitsi.org/jitsi-meet" ver="+mpajJhafj8jFogLBKsPbQfMgzU=" xmlns="http://jabber.org/protocol/caps"/>'
                                f"<jitsi_participant_codecList>vp9,vp8,h264,av1</jitsi_participant_codecList>"
                                f'<nick xmlns="http://jabber.org/protocol/nick">{user_data["firstName"]}</nick>'
                                f"<jitsi_participant_user-info>{escaped_ui}</jitsi_participant_user-info>"
                                f"<SourceInfo>{escaped_si}</SourceInfo>"
                                f"<jitsi_participant_video-size>{{&quot;width&quot;:960,&quot;height&quot;:720}}</jitsi_participant_video-size>"
                                f"</presence>"
                            )
                            ping_task = asyncio.create_task(xmpp_client_ping_loop())
                            state = "JOINED"
                        if "<r xmlns" in message:
                            await ws.send(f"<a xmlns='urn:xmpp:sm:3' h='{handled_stanzas}'/>")
                        if "urn:xmpp:ping" in message and "get" in message and "<ping" in message:
                            match = re.search(r'id=["\']([^"\']+)["\']', message)
                            if match:
                                await ws.send(f'<iq type="result" id="{match.group(1)}" xmlns="jabber:client"/>')
                        if state == "JOINED" and "<iq" in message and ("type='set'" in message or 'type="set"' in message):
                            match_id = re.search(r'id=["\']([^"\']+)["\']', message)
                            match_from = re.search(r'from=["\']([^"\']+)["\']', message)
                            if match_id and match_from and "focus" in match_from.group(1):
                                await ws.send(f'<iq type="result" id="{match_id.group(1)}" to="{match_from.group(1)}" xmlns="jabber:client"/>')
                finally:
                    if ping_task:
                        ping_task.cancel()
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _run_bot(self, link: str, time_exit: int) -> None:
        user_data = self.get_user_context()
        if not user_data:
            return
        short_name = link.split("/")[-1]
        conference_id = self.get_room_info(short_name)
        if not conference_id:
            return
        self.send_activity(short_name)
        tasks = [
            asyncio.create_task(self._background_activity_loop(short_name)),
            asyncio.create_task(self._connect_system_ws(short_name, user_data["id"])),
            asyncio.create_task(self._connect_xmpp_ws(conference_id, user_data)),
        ]
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=time_exit)
        except asyncio.TimeoutError:
            pass


def connect(link: str, time_exit: int = 5, auth_file: str = "ktalk_auth.txt") -> None:
    asyncio.run(KTalkBot(auth_file=auth_file)._run_bot(link, time_exit))
