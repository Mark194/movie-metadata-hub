from typing import Any

from common import get_settings
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

settings = get_settings()

security = HTTPBearer()

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> dict[str, Any]:
    token = credentials.credentials
    try:
        payload = jwt.decode(
            token,
            settings.jwt.secret_key,
            algorithms=[settings.jwt.algorithm]
        )
        user_id = payload.get('sub')
        role = payload.get('role', 'user')
        if user_id is None:
            raise HTTPException(status_code=401, detail='Invalid token')
        return {'user_id': int(user_id), 'role': role}
    except JWTError:
        raise HTTPException(status_code=401, detail='Invalid token')

async def require_admin(current_user: dict = Depends(get_current_user)):

    if current_user['role'] != 'admin':
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Admin rights required'
        )
    return current_user