from services.celery_app import celery_app

async def send_notification_async(user_id: str, template_name: str, context: dict):
    celery_app.send_task(
        'services.tasks.send_notification',
        args=[user_id, template_name, context]
    )