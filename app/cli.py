import asyncio

import uvicorn

from app.core.config import get_settings
from app.main import app
from app.scripts.add_test_event import add_test_event
from app.scripts.parse_all_to_pg import main as parse_all_to_pg
from app.workers.bot_service import main as worker_main


def run_api() -> None:
    settings = get_settings()
    uvicorn.run(
        app,
        host=settings.app_host,
        port=settings.app_port,
    )


def run_worker() -> None:
    asyncio.run(worker_main())


def run_parser() -> None:
    parse_all_to_pg()


def run_add_test_event() -> None:
    add_test_event()
