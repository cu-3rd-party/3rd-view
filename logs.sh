#!/bin/bash

# Читаем параметры:
# $1 - имя сервиса (web или worker), если пусто - покажет все.
# $2 - количество строк (по умолчанию 100).

SERVICE=$1
LINES=${2:-100}

if [ -z "$SERVICE" ]; then
    echo "=== Логи всех сервисов (последние $LINES строк) ==="
    echo "Для выхода нажмите Ctrl+C"
    docker compose logs -f --tail="$LINES"
else
    echo "=== Логи сервиса $SERVICE (последние $LINES строк) ==="
    echo "Для выхода нажмите Ctrl+C"
    docker compose logs -f --tail="$LINES" "$SERVICE"
fi
