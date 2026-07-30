from common import get_logger

logger = get_logger(__name__)


def send_email(to: str, subject: str, body: str):
    logger.info(f"Sending EMAIL to {to}: {subject}")
    # Здесь интеграция с реальным провайдером
    return True


def send_sms(phone: str, body: str):
    logger.info(f"Sending SMS to {phone}: {body}")
    return True


def send_push(user_id: int, title: str, body: str):
    logger.info(f"Sending PUSH to user {user_id}: {title}")
    return True
