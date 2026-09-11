"""Rebuild the course catalog and teacher directory from parsed calendar events.

The catalog used to come from a hand-maintained CSV of TiMe channel links, which
goes stale every semester. Search does not actually need it: /api/teachers and
/api/events both match on calendar_events.event_name and .teacher_names, and the
courses table only supplies the list of names to pick from. So derive it from the
events we already parsed.

Course rows are replaced wholesale (the old ones describe last semester). The
teacher directory is merged, never truncated -- teachers who only taught in a
past semester must keep their names or the archive loses its search.

Writes nothing unless --apply is passed.
"""

import argparse
import csv
import re
from datetime import date, datetime

from app.db import db_connection, init_db


LESSON_TYPES = (
    "Лекция",
    "Семинар",
    "Практика",
    "Практикум",
    "Лабораторная работа",
    "Лабораторная",
    "Консультация",
    "Экзамен",
    "Зачёт",
    "Зачет",
    "Коллоквиум",
    "Контрольная",
    "Защита",
    "Внутреннее мероприятие",
    "Внешнее мероприятие",
    "Другое",
)
TYPE_RE = re.compile(r",\s*(?:" + "|".join(LESSON_TYPES) + r")\b.*$")
LEADING_MARKS_RE = re.compile(r"^[\W_]+", re.UNICODE)
SKIP_EVENT_NAMES = {"Занят", "Событие скрыто"}

STAFF_STOP_WORDS = (
    # аудитории и железо
    "audience-", "room", "plasma", "board", "robot", "21-school.ru",
    # служебные ящики: timetable@ стоит в участниках почти каждой пары и иначе
    # прописался бы преподавателем всему расписанию
    "admin", "info@", "timetable@", "noreply", "no-reply", "support@", "testoviy",
)
# Преподавателя определяем по белому списку доменов, а не по принципу "всё, что не
# студент". Мы обходим личные календари целиком, и в участники попадает кто угодно:
# одна корпоративная встреча на 1296 человек занесла 1289 адресов @techvill.ru.
STAFF_DOMAINS = ("@centraluniversity.ru", "@cu.ru")


def subject_of(event_name: str) -> tuple[str, bool]:
    """'🔴 Основы статистики, Семинар, B702 (Дукат)' -> ('Основы статистики', True).

    The flag says whether the university naming convention was recognised. Names
    without it are mostly students' own calendar entries -- "работа", "прорешать
    кр" -- since we walk whole personal calendars, not just class schedules.
    """
    name = LEADING_MARKS_RE.sub("", (event_name or "").strip())
    cut = TYPE_RE.sub("", name).strip(" ,")
    if cut and cut != name:
        return cut, True
    parts = [part.strip() for part in name.split(",")]
    if len(parts) >= 3:
        return parts[0], False
    return name, False


def semester_of(start_time: str) -> str:
    try:
        moment = datetime.fromisoformat((start_time or "").replace("Z", "+00:00"))
    except ValueError:
        return "Без семестра"
    return f"{'Осень' if moment.month >= 8 else 'Весна'} {moment.year}"


def is_staff_email(email: str) -> bool:
    address = (email or "").lower().strip()
    if not address.endswith(STAFF_DOMAINS):
        return False
    return not any(word in address for word in STAFF_STOP_WORDS)


def pretty_fallback(email: str) -> str:
    return email.split("@")[0].title()


def load_names(path: str | None) -> dict[str, str]:
    """email -> 'Фамилия Имя', from a two-column csv/tsv."""
    if not path:
        return {}
    names: dict[str, str] = {}
    with open(path, encoding="utf-8-sig", newline="") as file_obj:
        sample = file_obj.read(4096)
        file_obj.seek(0)
        delimiter = "\t" if "\t" in sample.splitlines()[0] else ","
        for row in csv.reader(file_obj, delimiter=delimiter):
            if len(row) < 2:
                continue
            email, full_name = row[0].strip().lower(), row[1].strip()
            if email and full_name:
                names[email] = full_name
    return names


def collect_subjects(cur, min_attendees: int, since: str) -> dict[str, set[str]]:
    """Subjects worth putting in the catalog, mapped to the semesters they run in.

    A name is a class if it follows the convention; a name that does not can still
    earn its place by drawing a crowd (guest lectures, ВКР seminars). Everything
    else is someone's personal reminder and stays out.

    Only classes from `since` onwards count. The events table keeps every semester
    we ever parsed, and a catalog of subjects that finished months ago is a list of
    things the student cannot attend -- last time it was 619 dead entries out of 903.
    """
    cur.execute(
        "SELECT event_name, start_time, total_attendees FROM calendar_events WHERE start_time >= %s",
        (since,),
    )
    found: dict[str, dict] = {}
    for event_name, start_time, attendees in cur.fetchall():
        if not event_name or event_name in SKIP_EVENT_NAMES:
            continue
        subject, typed = subject_of(event_name)
        if not subject:
            continue
        entry = found.setdefault(subject, {"semesters": set(), "typed": False, "attendees": 0})
        entry["semesters"].add(semester_of(start_time))
        entry["typed"] = entry["typed"] or typed
        entry["attendees"] = max(entry["attendees"], attendees or 0)
    return {
        subject: entry["semesters"]
        for subject, entry in found.items()
        if entry["typed"] or entry["attendees"] >= min_attendees
    }


def collect_staff(cur, min_attendees: int) -> set[str]:
    """Staff seen in the attendee list of an actual class, not of any event at all."""
    cur.execute(
        "SELECT event_name, attendees_emails, total_attendees FROM calendar_events "
        "WHERE attendees_emails IS NOT NULL AND attendees_emails != ''"
    )
    staff: set[str] = set()
    for event_name, raw, attendees in cur.fetchall():
        if not event_name or event_name in SKIP_EVENT_NAMES:
            continue
        _, typed = subject_of(event_name)
        if not typed and (attendees or 0) < min_attendees:
            continue
        for item in raw.split(","):
            email = item.strip().lower()
            if is_staff_email(email):
                staff.add(email)
    return staff


def resolve_teacher_names(cur) -> int:
    cur.execute("SELECT email, full_name FROM teachers")
    directory = {row[0].strip().lower(): (row[1] or "").strip() for row in cur.fetchall()}

    cur.execute(
        "SELECT event_id, instance_start_ts, attendees_emails, teacher_names FROM calendar_events "
        "WHERE attendees_emails IS NOT NULL AND attendees_emails != ''"
    )
    changed = 0
    pending = []
    for event_id, instance_ts, attendees, current in cur.fetchall():
        matched = {directory[e.strip().lower()] for e in attendees.split(",") if e.strip().lower() in directory}
        resolved = ", ".join(sorted(matched)) if matched else "Не определен"
        if resolved != (current or ""):
            changed += 1
            pending.append((resolved, event_id, instance_ts))

    for resolved, event_id, instance_ts in pending:
        cur.execute(
            "UPDATE calendar_events SET teacher_names = %s "
            "WHERE event_id = %s AND instance_start_ts = %s",
            (resolved, event_id, instance_ts),
        )
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--names-file", default=None, help="csv/tsv: email,Фамилия Имя")
    parser.add_argument(
        "--since",
        default=date.today().isoformat(),
        help="YYYY-MM-DD: в каталог попадают предметы, у которых есть пары с этой даты (по умолчанию сегодня)",
    )
    parser.add_argument(
        "--min-attendees",
        type=int,
        default=20,
        help="сколько участников должно быть у события с непривычным именем, чтобы попасть в каталог",
    )
    parser.add_argument("--apply", action="store_true", help="без него скрипт только показывает, что сделает")
    args = parser.parse_args()

    init_db()
    names = load_names(args.names_file)
    if args.names_file:
        print(f"Имён загружено из файла: {len(names)}")

    with db_connection() as conn:
        with conn.cursor() as cur:
            subjects = collect_subjects(cur, args.min_attendees, args.since)
            staff = collect_staff(cur, args.min_attendees)
            cur.execute("SELECT email, full_name FROM teachers")
            known_teachers = {row[0].strip().lower(): (row[1] or "").strip() for row in cur.fetchall()}
            cur.execute("SELECT count(*) FROM courses")
            old_courses = cur.fetchone()[0]

            by_semester: dict[str, int] = {}
            for semesters in subjects.values():
                for semester in semesters:
                    by_semester[semester] = by_semester.get(semester, 0) + 1

            new_teachers = sorted(staff - set(known_teachers))
            renamed = [
                email
                for email in staff & set(known_teachers)
                if email in names and names[email] != known_teachers[email]
            ]

            print(f"Предметов с парами от {args.since}: {len(subjects)}")
            for semester, count in sorted(by_semester.items()):
                print(f"   {semester}: {count}")
            print(f"Курсов в таблице сейчас: {old_courses} -> станет {sum(len(s) for s in subjects.values())}")
            print(f"Преподавателей: сейчас {len(known_teachers)}, новых {len(new_teachers)}, переименуем {len(renamed)}")
            print(f"   имя из файла есть у {len(set(new_teachers) & set(names))} из {len(new_teachers)} новых")

            # Пишем всегда: без --apply транзакция откатится в конце. Иначе предпросмотр
            # пересчёта teacher_names врал бы -- он читает справочник, который мы тут же
            # и наполняем.
            cur.execute("DELETE FROM courses")
            for subject, semesters in sorted(subjects.items()):
                for semester in sorted(semesters):
                    cur.execute(
                        """
                        INSERT INTO courses (track, course_year, level, name, search_query, link)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (semester, "", "", subject, subject, ""),
                    )
            for email in staff:
                full_name = names.get(email) or known_teachers.get(email) or pretty_fallback(email)
                cur.execute(
                    """
                    INSERT INTO teachers (email, full_name) VALUES (%s, %s)
                    ON CONFLICT (email) DO UPDATE SET full_name = EXCLUDED.full_name
                    """,
                    (email, full_name),
                )

            changed = resolve_teacher_names(cur)
            print(f"Событий с изменившимся teacher_names: {changed}")

        if args.apply:
            conn.commit()
            print("Записано.")
        else:
            conn.rollback()
            print("Это был просмотр. Для записи добавьте --apply")


if __name__ == "__main__":
    main()
