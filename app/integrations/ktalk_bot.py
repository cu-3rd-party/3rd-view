from pathlib import Path

import ktalk_bot


def _load_auth_data(auth_file: str = "ktalk_auth.txt", auth_value: str | None = None) -> tuple[str, str | None]:
    raw_value = (auth_value or "").strip()
    if not raw_value:
        raw_value = Path(auth_file).read_text(encoding="utf-8").strip()
    if not raw_value:
        raise ValueError(f"KTalk auth file {auth_file} is empty")
    if raw_value.startswith("Session "):
        return "", raw_value.split(" ", 1)[1]
    return raw_value, None


def create_client(
    auth_file: str = "ktalk_auth.txt",
    room_link: str | None = None,
    auth_value: str | None = None,
) -> ktalk_bot.KTalkClient:
    cookie_header, session_token = _load_auth_data(auth_file, auth_value=auth_value)
    return ktalk_bot.create_engine(cookie_header, room_link=room_link, session_token=session_token)


def connect(link: str, time_exit: int = 5, auth_file: str = "ktalk_auth.txt", auth_value: str | None = None) -> None:
    create_client(auth_file=auth_file, room_link=link, auth_value=auth_value).join_room(duration_seconds=time_exit)
