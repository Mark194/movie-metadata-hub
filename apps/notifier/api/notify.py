from http import HTTPStatus
from http.client import HTTPException

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy.orm import Session

from api.schemas import EventPayload, BroadcastPayload, PersonalizedPayload, FreePayload
from db.postgres import get_db
from models.notify import Notification, Template
from services.celery_tasks import send_notification, broadcast_notification

router = APIRouter()


@router.post('/notify/event')
async def handle_event(payload: EventPayload, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    # Определяем шаблон и параметры в зависимости от event_type
    if payload.event_type == 'user_registered':
        # Отправляем приветственное письмо конкретному пользователю
        if not payload.user_id:
            raise HTTPException(HTTPStatus.NOT_FOUND, 'user_id required for user_registered')
        # Ищем шаблон с именем 'user_registered'
        template = db.query(Template).filter(Template.name == 'user_registered').first()
        if not template:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, 'Template for user_registered not configured')
        send_notification.delay(payload.user_id, template_id=template.id)
        return {'status': 'accepted', 'event': 'user_registered', 'user_id': payload.user_id}

    elif payload.event_type == 'new_movie':
        # Отправляем всем пользователям уведомление о новом фильме
        template = db.query(Template).filter(Template.name == 'new_movie').first()
        if not template:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, 'Template for new_movie not configured')
        context = {'movie_id': payload.movie_id} if payload.movie_id else {}
        broadcast_notification.delay(template.id, context=context)
        return {'status': 'accepted', 'event': 'new_movie'}

    else:
        raise HTTPException(HTTPStatus.NOT_FOUND, f'Unsupported event_type: {payload.event_type}')


@router.post('/notify/broadcast')
async def broadcast(payload: BroadcastPayload):
    broadcast_notification.delay(
        template_id=payload.template_id,
        context=payload.context,
        type_=payload.type,
        subject=payload.subject,
        body=payload.body,
    )
    return {'status': 'accepted'}


@router.post('/notify/personalized')
async def personalized(payload: PersonalizedPayload):
    send_notification.delay(
        user_id=payload.user_id,
        template_id=payload.template_id,
        context=payload.context,
        type_=payload.type,
        subject=payload.subject,
        body=payload.body,
    )
    return {'status': 'accepted'}


@router.post('/notify/free')
async def free_notification(payload: FreePayload):
    send_notification.delay(
        user_id=payload.user_id,
        template_id=payload.template_id,
        subject=payload.subject,
        body=payload.text,
        type_=payload.type,
    )
    return {'status': 'accepted'}


@router.get('/notifications/{user_id}')
async def get_user_notifications(user_id: int, limit: int = 10, db: Session = Depends(get_db)):
    notifs = db.query(Notification).filter(Notification.user_id == user_id) \
        .order_by(Notification.created_at.desc()).limit(limit).all()
    result = []
    for n in notifs:
        result.append({
            'id': n.id,
            'subject': n.subject,
            'body': n.body,
            'type': n.type,
            'status': n.status,
            'created_at': n.created_at.isoformat() if n.created_at else None,
            'sent_at': n.sent_at.isoformat() if n.sent_at else None,
        })
    return {'user_id': user_id, 'notifications': result}
