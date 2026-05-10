from uuid import UUID

from pydantic import BaseModel, Field


class RegistrationParams(BaseModel):
    login: str = Field(..., min_length=3, max_length=255)
    password: str = Field(..., min_length=6)
    first_name: str | None = None
    last_name: str | None = None


class AuthParams(BaseModel):
    login: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = 'bearer'


class RefreshParams(BaseModel):
    refresh_token: str


class LogoutParams(BaseModel):
    refresh_token: str


class RoleCreate(BaseModel):
    name: str
    description: str | None = None


class RoleUpdate(BaseModel):
    name: str | None = None
    description: str | None = None


class RoleResponse(BaseModel):
    id: UUID
    name: str
    description: str | None


class PermissionAssign(BaseModel):
    permission_id: UUID


class UserRoleAssign(BaseModel):
    user_id: UUID
    role_id: UUID
