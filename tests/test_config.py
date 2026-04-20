from app.core.config import Settings


def test_settings_build_time_headers(monkeypatch):
    monkeypatch.setenv("TIME_COOKIE", "cookie-value")
    monkeypatch.setenv("TIME_CSRF", "csrf-value")
    settings = Settings()
    assert settings.time_headers["cookie"] == "cookie-value"
    assert settings.time_headers["x-csrf-token"] == "csrf-value"
