from celery import Celery

from common.settings import get_settings

settings = get_settings()

FORMAT = 'json'

celery_app = Celery(
    'billing_worker',
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)
celery_app.conf.update(
    task_serializer=FORMAT,
    accept_content=[FORMAT],
    result_serializer=FORMAT,
    timezone='UTC',
    enable_utc=True,
)

celery_app.autodiscover_tasks(['services.tasks'])