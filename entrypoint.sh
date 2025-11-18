#!/usr/bin/env bash
set -e

echo "🚀 Starting OTC Collector..."
python /app/update/t_collector.py &

echo "🤖 Starting Bot..."
exec python /app/update/bot.py