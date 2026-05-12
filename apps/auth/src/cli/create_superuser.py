# cli/create_superuser.py
import asyncio
import sys
from pathlib import Path

# Добавляем корень проекта в путь, чтобы импортировать модули
sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.settings import get_settings
from db.postgres import init_postgres, close_postgres, _sessionmaker
from models.user import User
from werkzeug.security import generate_password_hash


async def create_superuser(login: str, password: str, first_name: str = "", last_name: str = ""):
    settings = get_settings()
    await init_postgres(settings.postgres.async_db_url)

    async with _sessionmaker() as session:
        from sqlalchemy import select
        stmt = select(User).where(User.login == login)
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()
        if existing:
            print(f"Пользователь {login} уже существует.")
            if existing.is_superuser:
                print("Он уже является суперпользователем.")
            else:
                # Можно предложить сделать его суперпользователем
                existing.is_superuser = True
                await session.commit()
                print(f"Пользователь {login} теперь суперпользователь.")
            await close_postgres()
            return

        # Создаём нового суперпользователя
        new_user = User(
            login=login,
            password=generate_password_hash(password),
            first_name=first_name,
            last_name=last_name,
            is_superuser=True
        )
        session.add(new_user)
        await session.commit()
        print(f"Суперпользователь {login} успешно создан.")

    await close_postgres()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Создание суперпользователя")
    parser.add_argument("login", help="Логин")
    parser.add_argument("password", help="Пароль")
    parser.add_argument("--first-name", default="", help="Имя")
    parser.add_argument("--last-name", default="", help="Фамилия")
    args = parser.parse_args()

    asyncio.run(create_superuser(args.login, args.password, args.first_name, args.last_name))