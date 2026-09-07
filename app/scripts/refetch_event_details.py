"""Re-fetch link_description and attendees_emails for events that came back empty.

Yandex intermittently answers a detail request with something that is not JSON;
the API client swallows it and returns empty values, so a long parse run ends with
a small tail of events that have no broadcast link and no attendees. Those are the
events nobody can find a stream for, so pick them up in a second pass.

Writes nothing unless --apply is passed.
"""

import argparse
import time

from app.core.config import get_settings
from app.db import db_connection, init_db
from app.integrations.yandex_api import YandexCalendarAPI


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default="2026-09-01", help="YYYY-MM-DD, нижняя граница по start_time")
    parser.add_argument("--limit", type=int, default=0, help="0 -- без ограничения")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    init_db()
    settings = get_settings()
    api = YandexCalendarAPI(cookie_path=str(settings.cookie_file))

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, instance_start_ts FROM calendar_events
                WHERE start_time >= %s
                  AND (link_description IS NULL OR link_description = '')
                ORDER BY start_time
                """,
                (args.since,),
            )
            targets = [(row[0], row[1]) for row in cur.fetchall()]

    if args.limit:
        targets = targets[: args.limit]
    print(f"Пар без деталей: {len(targets)}")
    if not targets:
        return

    recovered: list[tuple[str, str, str, str]] = []
    still_empty = 0
    for index, (event_id, instance_ts) in enumerate(targets, 1):
        if index % 25 == 0:
            print(f"  {index} / {len(targets)}", flush=True)
        link_desc, attendees = api.get_event_details_by_id(event_id, instance_ts)
        if link_desc or attendees:
            recovered.append((link_desc, ",".join(attendees), event_id, instance_ts))
        else:
            still_empty += 1
        time.sleep(0.4)

    with_link = sum(1 for row in recovered if "ktalk" in (row[0] or ""))
    print(f"Восстановлено: {len(recovered)}; из них со ссылкой на KTalk: {with_link}; так и пусто: {still_empty}")

    if not args.apply:
        print("Это был просмотр. Для записи добавьте --apply")
        return

    with db_connection() as conn:
        with conn.cursor() as cur:
            for link_desc, attendees, event_id, instance_ts in recovered:
                cur.execute(
                    """
                    UPDATE calendar_events
                    SET link_description = %s, attendees_emails = %s
                    WHERE event_id = %s AND instance_start_ts = %s
                    """,
                    (link_desc, attendees, event_id, instance_ts),
                )
        conn.commit()
    print("Записано.")


if __name__ == "__main__":
    main()
