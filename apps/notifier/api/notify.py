from http import HTTPStatus

from db.postgres import get_db
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from models.notify import Notification, Template
from services.celery_tasks import broadcast_notification, send_notification
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_current_user, require_admin
from api.schemas import BroadcastPayload, EventPayload, FreePayload, PersonalizedPayload

router = APIRouter()


@router.post("/notify/event")
async def handle_event(
    payload: EventPayload,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    # Определяем шаблон и параметры в зависимости от event_type
    if payload.event_type == "user_registered":
        # Отправляем приветственное письмо конкретному пользователю
        if not payload.user_id:
            raise HTTPException(
                HTTPStatus.NOT_FOUND, "user_id required for user_registered"
            )
        # Ищем шаблон с именем 'user_registered' асинхронно
        stmt = select(Template).where(Template.name == "user_registered")
        result = await db.execute(stmt)
        template = result.scalar_one_or_none()
        if not template:
            raise HTTPException(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "Template for user_registered not configured",
            )
        send_notification.delay(payload.user_id, template_id=template.id)
        return {
            "status": "accepted",
            "event": "user_registered",
            "user_id": payload.user_id,
        }

    elif payload.event_type == "new_movie":
        # Отправляем всем пользователям уведомление о новом фильме
        stmt = select(Template).where(Template.name == "new_movie")
        result = await db.execute(stmt)
        template = result.scalar_one_or_none()
        if not template:
            raise HTTPException(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "Template for new_movie not configured",
            )
        context = {"movie_id": payload.movie_id} if payload.movie_id else {}
        broadcast_notification.delay(template.id, context=context)
        return {"status": "accepted", "event": "new_movie"}

    else:
        raise HTTPException(
            HTTPStatus.NOT_FOUND, f"Unsupported event_type: {payload.event_type}"
        )


@router.post("/notify/broadcast")
async def broadcast(payload: BroadcastPayload, admin: dict = Depends(require_admin)):
    broadcast_notification.delay(
        template_id=payload.template_id,
        context=payload.context,
        type_=payload.type,
        subject=payload.subject,
        body=payload.body,
    )
    return {"status": "accepted"}


@router.post("/notify/personalized")
async def personalized(
    payload: PersonalizedPayload, current_user: dict = Depends(get_current_user)
):
    if payload.user_id != current_user["user_id"]:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail="Not allowed to send for another user",
        )

    send_notification.delay(
        user_id=payload.user_id,
        template_id=payload.template_id,
        context=payload.context,
        type_=payload.type,
        subject=payload.subject,
        body=payload.body,
    )
    return {"status": "accepted"}


@router.post("/notify/free")
async def free_notification(
    payload: FreePayload, current_user: dict = Depends(get_current_user)
):
    if payload.user_id != current_user["user_id"]:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail="Not allowed to send for another user",
        )

    send_notification.delay(
        user_id=payload.user_id,
        template_id=payload.template_id,
        subject=payload.subject,
        body=payload.text,
        type_=payload.type,
    )
    return {"status": "accepted"}


@router.get("/notifications/{user_id}")
async def get_user_notifications(
    user_id: int,
    limit: int = 10,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):

    if user_id != current_user["user_id"] and current_user["role"] != "admin":
        raise HTTPException(status_code=HTTPStatus.FORBIDDEN, detail="Access denied")

    stmt = (
        select(Notification)
        .where(Notification.user_id == user_id)
        .order_by(Notification.created_at.desc())
        .limit(limit)
    )
    result = []
    result_operation = await db.execute(stmt)
    notifs = result_operation.scalars().all()
    for n in notifs:
        result.append(
            {
                "id": n.id,
                "subject": n.subject,
                "body": n.body,
                "type": n.type,
                "status": n.status,
                "created_at": n.created_at.isoformat() if n.created_at else None,
                "sent_at": n.sent_at.isoformat() if n.sent_at else None,
            }
        )
    return {"user_id": user_id, "notifications": result}
