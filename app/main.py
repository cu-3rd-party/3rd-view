from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.router import api_router
from app.core.config import get_settings, BASE_DIR
from app.core.logging import configure_logging
from app.db import init_db


def create_app(initialize_database: bool = True) -> FastAPI:
    settings = get_settings()
    configure_logging()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if initialize_database:
            init_db()
        yield

    app = FastAPI(
        title=settings.app_name, 
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    static_dir = BASE_DIR / "app" / "static"
    static_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    app.include_router(api_router)
    return app


app = create_app()
