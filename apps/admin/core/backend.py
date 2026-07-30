from django.contrib.auth.backends import BaseBackend
from django.contrib.auth.models import User as DjangoUser

from core.auth import get_auth_client


class AuthServiceBackend(BaseBackend):
    def authenticate(self, request, username=None, password=None):
        client = get_auth_client()
        tokens = client.login(username, password)
        if tokens:
            user, _created = DjangoUser.objects.get_or_create(username=username)
            user.is_staff = True
            user.save()

            request.session['access_token'] = tokens['access_token']
            request.session['refresh_token'] = tokens['refresh_token']
            return user
        return None

    def get_user(self, user_id):
        try:
            return DjangoUser.objects.get(pk=user_id)
        except DjangoUser.DoesNotExist:
            return None