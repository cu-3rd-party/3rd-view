from app.db import db_connection, init_db


def add_test_event() -> None:
    init_db()
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO calendar_events
                (event_id, instance_start_ts, start_time, end_time, event_name, total_attendees, link_description, attendees_emails, teacher_names)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id, instance_start_ts) DO UPDATE SET
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                event_name = EXCLUDED.event_name,
                total_attendees = EXCLUDED.total_attendees,
                link_description = EXCLUDED.link_description,
                attendees_emails = EXCLUDED.attendees_emails,
                teacher_names = EXCLUDED.teacher_names
                """,
                (
                    "test_manual_event_postgres_001",
                    "2026-04-16T20:45:00.000Z",
                    "2026-04-16T23:45:00+03:00",
                    "2026-04-16T23:47:00+03:00",
                    "Дискретная математика, последняя тестовая пара (PG)",
                    1,
                    "https://centraluniversity.ktalk.ru/w33jrhh8aprj",
                    "",
                    "Тестовый Преподаватель Postgres",
                ),
            )
        conn.commit()


if __name__ == "__main__":
    add_test_event()
