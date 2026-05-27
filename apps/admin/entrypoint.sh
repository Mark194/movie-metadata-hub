#!/bin/sh
python manage.py collectstatic --noinput
exec python -m uvicorn config.asgi:application --host 0.0.0.0 --port 8000