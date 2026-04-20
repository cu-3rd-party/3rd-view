import json


EVENT_COLORS = ["#3788d8", "#28a745", "#dc3545", "#fd7e14", "#6f42c1", "#20c997"]


def emit(msg_type: str, data: object) -> str:
    return json.dumps({"type": msg_type, "data": data}) + "\n"


def event_color(title: str) -> str:
    return EVENT_COLORS[hash(title.split(",")[0]) % len(EVENT_COLORS)]
