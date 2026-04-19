# Файл: schemas.py
from pydantic import BaseModel
from typing import List, Optional

class CourseUpdateModel(BaseModel):
    search_query: str
    link: str

class NewTeacherModel(BaseModel):
    email: str
    full_name: str

class ManualRecordingModel(BaseModel):
    yandex_event_id: str
    recording_date: str
    recording_url: str

class CourseFilter(BaseModel):
    query: str
    teachers: List[str]

class EventsRequest(BaseModel):
    start: str
    end: str
    filters: List[CourseFilter]

class RegisterRequest(BaseModel):
    email: str
    password: str

class VerifyRequest(BaseModel):
    email: str
    code: str

class LoginRequest(BaseModel):
    email: str
    password: str