import datetime
import secrets
import smtplib
from datetime import timedelta, timezone
from email.mime.text import MIMEText

import jwt
from fastapi import Depends, HTTPException, Security
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBasic,
    HTTPBasicCredentials,
    HTTPBearer,
)
from passlib.context import CryptContext

from app.core.config import get_settings


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security_jwt = HTTPBearer()
security_jwt_opt = HTTPBearer(auto_error=False)
security_basic_opt = HTTPBasic(auto_error=False)
security_basic = HTTPBasic()


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict) -> str:
    settings = get_settings()
    to_encode = data.copy()
    expire = datetime.datetime.now(timezone.utc) + timedelta(days=settings.access_token_days)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)


def send_verification_email(to_email: str, code: str) -> None:
    settings = get_settings()
    msg = MIMEText(f"Ваш код для входа: {code}")
    msg["Subject"] = "Регистрация"
    msg["From"] = settings.smtp_user
    msg["To"] = to_email

    with smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port) as server:
        server.login(settings.smtp_user, settings.smtp_password)
        server.send_message(msg)


async def verify_user_or_admin(
    jwt_creds: HTTPAuthorizationCredentials = Security(security_jwt_opt),
    basic_creds: HTTPBasicCredentials = Security(security_basic_opt),
) -> dict[str, str]:
    settings = get_settings()
    if jwt_creds:
        try:
            payload = jwt.decode(
                jwt_creds.credentials,
                settings.secret_key,
                algorithms=[settings.algorithm],
            )
            email = payload.get("sub")
            if email:
                return {"role": "student", "email": email}
        except jwt.PyJWTError:
            pass

    if basic_creds:
        if secrets.compare_digest(basic_creds.username, settings.admin_username) and secrets.compare_digest(
            basic_creds.password,
            settings.admin_password,
        ):
            return {"role": "admin", "email": "admin"}

    raise HTTPException(status_code=401, detail="Необходима авторизация")


def verify_admin(credentials: HTTPBasicCredentials = Depends(security_basic)) -> str:
    settings = get_settings()
    if not (
        secrets.compare_digest(credentials.username, settings.admin_username)
        and secrets.compare_digest(credentials.password, settings.admin_password)
    ):
        raise HTTPException(
            status_code=401,
            detail="Неверный логин или пароль",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security_jwt)) -> str:
    settings = get_settings()
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.secret_key,
            algorithms=[settings.algorithm],
        )
        email = payload.get("sub")
        if not email:
            raise HTTPException(status_code=401, detail="Неверный токен")
        return email
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=401, detail="Токен недействителен") from exc
