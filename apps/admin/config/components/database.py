from common import get_settings

config = get_settings()

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": config.postgres.db,
        "USER": config.postgres.user,
        "PASSWORD": config.postgres.password,
        "HOST": config.postgres.host,
        "PORT": config.postgres.port,
        "OPTIONS": {
            "options": "-c search_path=public,content"
        },
    }
}
