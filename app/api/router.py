from fastapi import APIRouter

from app.api.routes import auth, courses, events, pages, recordings, student, teachers


api_router = APIRouter()
api_router.include_router(auth.router, tags=["auth"])
api_router.include_router(pages.router, tags=["pages"])
api_router.include_router(courses.router, tags=["courses"])
api_router.include_router(events.router, tags=["events"])
api_router.include_router(teachers.router, tags=["teachers"])
api_router.include_router(recordings.router, tags=["recordings"])
api_router.include_router(student.router, tags=["student"])
