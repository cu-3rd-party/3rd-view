from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse

from app.auth import verify_admin
from app.core.config import get_settings


router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def read_index() -> str:
    settings = get_settings()
    return (settings.templates_dir / "index.html").read_text(encoding="utf-8")


@router.get("/admin", response_class=HTMLResponse)
async def read_admin(admin: str = Depends(verify_admin)) -> str:
    settings = get_settings()
    return (settings.templates_dir / "admin.html").read_text(encoding="utf-8")


@router.get("/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}
