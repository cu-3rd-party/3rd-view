import jwt

from app.auth import create_access_token
from app.core.config import get_settings


def test_create_access_token_contains_subject():
    token = create_access_token({"sub": "student@edu.centraluniversity.ru"})
    settings = get_settings()
    payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
    assert payload["sub"] == "student@edu.centraluniversity.ru"
    assert "exp" in payload
