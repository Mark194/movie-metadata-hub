from fastapi import APIRouter

router = APIRouter()


@router.put('/user/login')
async def change_login():
    pass


@router.put('/user/password')
async def change_password():
    pass


@router.get('/user/login-history')
async def login_history():
    pass


@router.post('/users/{user_id}/roles')
async def add_role_to_user():
    pass


@router.delete('/users/{user_id}/roles/{role_id}')
async def remove_role_from_user():
    pass
