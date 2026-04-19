# Файл: auth.py
import datetime
import random
import string
import smtplib
import secrets
import jwt
from email.mime.text import MIMEText
from datetime import timezone, timedelta
from passlib.context import CryptContext
from fastapi import HTTPException, Depends, Security, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials, HTTPBearer, HTTPAuthorizationCredentials

from config import Config

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security_jwt = HTTPBearer()
security_jwt_opt = HTTPBearer(auto_error=False)
security_basic_opt = HTTPBasic(auto_error=False)
security_basic = HTTPBasic()

def get_password_hash(password):
    return pwd_context.hash(password)

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.datetime.now(timezone.utc) + timedelta(days=30)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, Config.SECRET_KEY, algorithm=Config.ALGORITHM)

def send_verification_email(to_email: str, code: str):
    msg = MIMEText(f"Ваш код для входа: {code}")
    msg['Subject'] = "Регистрация"
    msg['From'] = Config.SMTP_USER
    msg['To'] = to_email

    with smtplib.SMTP_SSL(Config.SMTP_HOST, Config.SMTP_PORT) as server:
        server.login(Config.SMTP_USER, Config.SMTP_PASSWORD)
        server.send_message(msg)

# Зависимости FastAPI
async def verify_user_or_admin(
    jwt_creds: HTTPAuthorizationCredentials = Security(security_jwt_opt),
    basic_creds: HTTPBasicCredentials = Security(security_basic_opt)
):
    if jwt_creds:
        try:
            payload = jwt.decode(jwt_creds.credentials, Config.SECRET_KEY, algorithms=[Config.ALGORITHM])
            email = payload.get("sub")
            if email: return {"role": "student", "email": email}
        except jwt.PyJWTError: pass 

    if basic_creds:
        if secrets.compare_digest(basic_creds.username, Config.ADMIN_USERNAME) and \
           secrets.compare_digest(basic_creds.password, Config.ADMIN_PASSWORD):
            return {"role": "admin", "email": "admin"}

    raise HTTPException(status_code=401, detail="Необходима авторизация")

def verify_admin(credentials: HTTPBasicCredentials = Depends(security_basic)):
    if not (secrets.compare_digest(credentials.username, Config.ADMIN_USERNAME) and 
            secrets.compare_digest(credentials.password, Config.ADMIN_PASSWORD)):
        raise HTTPException(status_code=401, detail="Неверный логин или пароль", headers={"WWW-Authenticate": "Basic"})
    return credentials.username

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security_jwt)):
    try:
        payload = jwt.decode(credentials.credentials, Config.SECRET_KEY, algorithms=[Config.ALGORITHM])
        email = payload.get("sub")
        if not email: raise HTTPException(status_code=401, detail="Неверный токен")
        return email
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Токен недействителен")