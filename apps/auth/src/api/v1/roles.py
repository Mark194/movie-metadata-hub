from fastapi import APIRouter

router = APIRouter()


@router.get('/roles')
async def get_roles():
    pass


@router.post('/roles')
async def create_role():
    pass


@router.get('/roles/{id}')
async def change_role(id):
    pass


@router.delete('/roles/{id}')
async def delete_role(id):
    pass


