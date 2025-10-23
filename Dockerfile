# Используем официальный минимальный образ Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем зависимости
COPY requirements.txt /app/

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код проекта
COPY . /app

# Настраиваем среду
ENV PYTHONUNBUFFERED=1

# Точка входа
ENTRYPOINT ["/app/entrypoint.sh"]