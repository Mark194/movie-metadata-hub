#!/bin/bash
cd /app/src

# Если нет версий миграций, создаём autogenerate
if [ ! -d "migrations/versions" ] || [ -z "$(ls -A migrations/versions)" ]; then
    echo "No migrations found, generating..."
    alembic revision --autogenerate -m "Initial"
fi

# Применяем миграции
alembic upgrade head
# Запускаем приложение
exec python -m uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4 --loop auto --http auto --log-level info