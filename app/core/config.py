from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "University Calendar API"
    app_host: str = "0.0.0.0"
    app_port: int = 8082
    allowed_origins: list[str] = Field(default_factory=lambda: ["*"])
    allowed_domains: list[str] = Field(
        default_factory=lambda: ["@edu.centraluniversity.ru", "@centraluniversity.ru"]
    )
    secret_key: str = "fallback_secret_change_me_please_32"
    algorithm: str = "HS256"
    access_token_days: int = 30
    admin_username: str = "admin"
    admin_password: str = "admin"
    db_name: str | None = None
    db_user: str | None = None
    db_password: str | None = None
    db_host: str = "127.0.0.1"
    db_port: int = 5432
    smtp_host: str | None = None
    smtp_port: int = 465
    smtp_user: str | None = None
    smtp_password: str | None = None
    time_team_id: str | None = None
    time_cookie: str | None = None
    time_csrf: str | None = None
    cookie_file: Path = BASE_DIR / "cookie.txt"
    ktalk_auth_file: Path = BASE_DIR / "ktalk_auth.txt"
    templates_dir: Path = BASE_DIR / "app" / "templates"

    @field_validator("allowed_origins", "allowed_domains", mode="before")
    @classmethod
    def parse_csv_list(cls, value: str | list[str]) -> list[str]:
        if isinstance(value, list):
            return value
        if not value:
            return []
        return [item.strip() for item in value.split(",") if item.strip()]

    @property
    def db_config(self) -> dict[str, str | int | None]:
        return {
            "dbname": self.db_name,
            "user": self.db_user,
            "password": self.db_password,
            "host": self.db_host,
            "port": self.db_port,
        }

    @property
    def time_headers(self) -> dict[str, str | None]:
        return {
            "accept": "application/json, text/plain, */*",
            "cookie": self.time_cookie,
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "x-csrf-token": self.time_csrf,
            "x-requested-with": "XMLHttpRequest",
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
