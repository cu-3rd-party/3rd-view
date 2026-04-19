import random
import string

from fastapi import APIRouter, HTTPException
from psycopg2.extras import RealDictCursor

from app.auth import create_access_token, get_password_hash, send_verification_email, verify_password
from app.core.config import get_settings
from app.db import db_connection
from app.schemas import LoginRequest, RegisterRequest, VerifyRequest


router = APIRouter(prefix="/api/auth")


@router.post("/register")
async def register_user(data: RegisterRequest) -> dict[str, str]:
    settings = get_settings()
    email = data.email.lower().strip()
    if not any(email.endswith(domain) for domain in settings.allowed_domains):
        raise HTTPException(status_code=400, detail="Разрешены только корпоративные почты")

    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (email,))
            user = cur.fetchone()
            if user and user["is_active"]:
                raise HTTPException(status_code=400, detail="Email уже зарегистрирован")

            code = "".join(random.choices(string.digits, k=6))
            hashed_pwd = get_password_hash(data.password)
            if user and not user["is_active"]:
                cur.execute("UPDATE users SET password_hash = %s WHERE email = %s", (hashed_pwd, email))
            else:
                cur.execute("INSERT INTO users (email, password_hash) VALUES (%s, %s)", (email, hashed_pwd))
            cur.execute(
                """
                INSERT INTO email_verifications (email, code, created_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (email) DO UPDATE SET code = EXCLUDED.code, created_at = CURRENT_TIMESTAMP
                """,
                (email, code),
            )
            try:
                send_verification_email(email, code)
            except Exception as exc:
                conn.rollback()
                raise HTTPException(status_code=500, detail=f"Ошибка отправки почты: {exc}") from exc
        conn.commit()
    return {"status": "ok", "message": "Код отправлен на почту"}


@router.post("/verify")
async def verify_user(data: VerifyRequest) -> dict[str, str]:
    email = data.email.lower().strip()
    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT code FROM email_verifications WHERE email = %s", (email,))
            row = cur.fetchone()
            if not row or row["code"] != data.code.strip():
                raise HTTPException(status_code=400, detail="Неверный код")
            cur.execute("UPDATE users SET is_active = TRUE WHERE email = %s", (email,))
            cur.execute("DELETE FROM email_verifications WHERE email = %s", (email,))
        conn.commit()
    return {"status": "ok"}


@router.post("/login")
async def login_user(data: LoginRequest) -> dict[str, str]:
    email = data.email.lower().strip()
    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (email,))
            user = cur.fetchone()
            if not user or not verify_password(data.password, user["password_hash"]):
                raise HTTPException(status_code=401, detail="Неверный логин или пароль")
            if not user["is_active"]:
                raise HTTPException(status_code=403, detail="Почта не подтверждена")
    return {"access_token": create_access_token({"sub": email})}
