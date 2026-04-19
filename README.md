# 🎓 cu 3rd view
(fully vibecoded expirement)
Здесь не всё дописано, скорее всего. Идея общая в том, что нужно получить файл new_courses.csv, а далее его отправить в базу данных postgres. Без этого ничего не получится.


Комплексная система для сбора, управления и отображения расписания университетских занятий. Проект автоматически парсит корпоративные календари (Яндекс), находит ссылки на видеоконференции, привязывает записи занятий и использует бота (на вебсокетах) для автоматического присутствия на парах в системе KTalk.

---

## 🌟 Основной функционал

- **Автоматический сбор базы:** Умный сканер (`full_parse.py`) для выгрузки всех преподавателей и каналов-предметов из корпоративного мессенджера TiMe напрямую в PostgreSQL.
- **Парсинг расписания:** Интеграция с Yandex Calendar API для извлечения пар и привязки их к конкретным студентам.
- **Умный поиск записей:** Фоновая синхронизация с KTalk API для поиска и прикрепления видеозаписей к прошедшим парам.
- **KTalk Bot:** Асинхронный бот на WebSockets, который "отсиживает" пары по расписанию для создания отметки присутствия/записи.
- **Личный кабинет студента:** Регистрация по корпоративной почте с подтверждением через код (SMTP Yandex), просмотр своего расписания и записей.
- **Панель администратора:** Запуск тотальной синхронизации, управление курсами, преподавателями и ручное добавление ссылок на видео.

---

## 🛠 Технологический стек

- **Backend:** Python 3.9+, [FastAPI](https://fastapi.tiangolo.com/), Uvicorn
- **База данных:** PostgreSQL (`psycopg2-binary`)
- **Авторизация:** JWT (для студентов), Basic Auth (для админов), Bcrypt (хеширование)
- **Связь и парсинг:** `requests`, `websockets`

---

## 📂 Структура проекта

project_root/
├── .env                    # Секреты и доступы (не коммитить!)
├── .gitignore              # Игнор-лист для Git
├── requirements.txt        # Список зависимостей
├── config.py               # Конфигурация приложения (загрузка из .env)
├── database.py             # Подключение и инициализация таблиц PostgreSQL
├── schemas.py              # Pydantic-модели для валидации данных
├── auth.py                 # Логика безопасности, токены и отправка email
├── main.py                 # Главный файл FastAPI (все веб-роуты)
├── bot_service.py          # Фоновый воркер (авто-заход на пары и синхронизация)
├── templates/              # HTML-шаблоны фронтенда
│   ├── index.html          # Дашборд студента
│   └── admin.html          # Админ-панель
├── integrations/           # Модули взаимодействия со сторонними API
│   ├── __init__.py
│   ├── yandex_api.py       # Клиент API Яндекс Календаря
│   ├── ktalk_api.py        # Клиент API истории конференций KTalk
│   └── ktalk_bot.py        # WebSocket-бот для KTalk
└── scripts/                # Утилиты и скрипты
    ├── full_parse.py       # Парсер базы TiMe (преподаватели и курсы)
    └── add_test_event.py   # Скрипт для ручного тестирования БД


---

## ⚙️ Установка и настройка (Локально)

### 1. Подготовка базы данных (PostgreSQL)
Убедитесь, что у вас установлен PostgreSQL. Создайте базу данных и пользователя:
```sql
CREATE DATABASE cu_view_db;
CREATE USER cu_view_user WITH PASSWORD 'strong_password';
GRANT ALL PRIVILEGES ON DATABASE cu_view_db TO cu_view_user;
-- Для PostgreSQL 15+
\c cu_view_db
GRANT ALL ON SCHEMA public TO cu_view_user;
```

### 2. Клонирование и зависимости
Создайте виртуальное окружение и установите библиотеки:
```bash
python -m venv venv
source venv/bin/activate  # Для Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Настройка доступов (Файлы ключей)
В корне проекта необходимо создать **три файла**:

1. **`.env`** — основной файл конфигурации:
   ```env
   SECRET_KEY=ВАШ_СЕКРЕТНЫЙ_КЛЮЧ_ДЛЯ_JWT
   ALGORITHM=HS256
   ADMIN_USERNAME=admin
   ADMIN_PASSWORD=ВАШ_ПАРОЛЬ_АДМИНА
   
   DB_NAME=cu_view_db
   DB_USER=cu_view_user
   DB_PASSWORD=PAROL_OT_BAZY)
   DB_HOST=127.0.0.1
   DB_PORT=5432
   
   SMTP_HOST=smtp.yandex.ru
   SMTP_PORT=465
   SMTP_USER=почта_отправителя@yandex.com
   SMTP_PASSWORD=пароль_приложения_яндекс
   
   TIME_TEAM_ID=id_команды
   TIME_COOKIE=полная_кука_авторизации
   TIME_CSRF=токен_csrf
   ```
2. **`cookie.txt`** — вставьте в него строку с Cookie-данными от Яндекс Календаря.
3. **`ktalk_auth.txt`** — вставьте токен авторизации от системы Толк (в формате `Session VOhszgu...`).

### 4. Первичное заполнение базы
Используйте готовый парсер, который вытащит преподавателей и курсы из мессенджера TiMe напрямую в БД:
```bash
python scripts/full_parse.py
```
*В меню выберите по очереди пункты `1` и `2`.*

---

## 🚀 Развертывание на сервере Linux (systemd)

Чтобы FastAPI сервер и фоновый бот работали непрерывно, их нужно добавить как системные сервисы. *Предполагается, что проект лежит в `/opt/cu-view`.*

### 1. Создание сервиса Web-сервера (FastAPI)
Создайте файл: `sudo nano /etc/systemd/system/cu-view-web.service`
Вставьте следующее содержимое:
```ini
[Unit]
Description=CU View Web Server (FastAPI)
After=network.target

[Service]
User=root
WorkingDirectory=/opt/cu-view
ExecStart=/opt/cu-view/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8082
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### 2. Создание сервиса Бота
Создайте файл: `sudo nano /etc/systemd/system/cu-view-bot.service`
Вставьте следующее содержимое:
```ini
[Unit]
Description=CU View KTalk Bot & Sync Worker
After=network.target

[Service]
User=root
WorkingDirectory=/opt/cu-view
ExecStart=/opt/cu-view/venv/bin/python bot_service.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 3. Запуск и автозагрузка
Обновите конфигурацию systemd, включите автозапуск при старте сервера и запустите сервисы:
```bash
sudo systemctl daemon-reload
sudo systemctl enable cu-view-web cu-view-bot
sudo systemctl start cu-view-web cu-view-bot
```

### 4. Просмотр логов
Если нужно проверить, как работают сервисы, используйте команды:
```bash
# Логи FastAPI (веб-сайт)
sudo journalctl -u cu-view-web -f

# Логи бота (заходы на пары и синхронизация видео)
sudo journalctl -u cu-view-bot -f
```

---

## 🌐 Интерфейсы

- **Студенческий интерфейс:** `http://ВАШ_IP:8082/`
- **Панель администратора:** `http://ВАШ_IP:8082/admin`
- **Документация API (Swagger):** `http://ВАШ_IP:8082/docs`

---