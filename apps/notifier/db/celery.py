from celery import Celery
from common import get_settings

settings = get_settings()

SERIALIZER_TYPE = "json"

celery_app = Celery(
    "notification_worker",
    broker=settings.notify.celery_url,
    backend=settings.notify.celery_backend,
)
celery_app.conf.update(
    task_serializer=SERIALIZER_TYPE,
    accept_content=(SERIALIZER_TYPE,),
    result_serializer=SERIALIZER_TYPE,
    timezone="UTC",
    enable_utc=True,
)
