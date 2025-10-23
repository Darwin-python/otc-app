#!/usr/bin/env bash
set -e

# Запускаем t_collector.py в фоне
python /app/update/t_collector.py &

# Запускаем bot.py (впереди, чтобы контейнер не завершился)
exec python /app/update/bot.py