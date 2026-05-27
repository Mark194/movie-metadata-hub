from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _

from .mixins import TimeStampedMixin, UUIDMixin

RATING_MINIMAL = 0
RATING_MAXIMUM = 100

MAX_LENGTH_NAME_FIELD = 255


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_("name"), max_length=MAX_LENGTH_NAME_FIELD)
    description = models.TextField(_("description"), blank=True)

    class Meta:
        db_table = 'content"."genre'
        verbose_name = _("Genre")
        verbose_name_plural = _("Genres")

    def __str__(self):
        return self.name


class FilmWork(UUIDMixin, TimeStampedMixin):
    class FilmWorkType(models.TextChoices):
        MOVIE = "movie"
        TV_SHOW = "tv_show"

    title = models.CharField(_("title"), max_length=MAX_LENGTH_NAME_FIELD)
    description = models.TextField(_("description"), blank=True)
    creation_date = models.DateField(_("creation_date"), auto_now_add=True)
    rating = models.FloatField(
        _("rating"),
        default=RATING_MINIMAL,
        validators=(
            MinValueValidator(RATING_MINIMAL),
            MaxValueValidator(RATING_MAXIMUM),
        ),
    )
    type = models.TextField(
        _("type"), choices=FilmWorkType.choices, default=FilmWorkType.MOVIE
    )
    genres = models.ManyToManyField("Genre", through="GenreFilmWork")
    persons = models.ManyToManyField("Person", through="PersonFilmWork")
    file_path = models.FileField(
        _("file"),
        blank=True,
        null=True,
        upload_to="movies/")

    class Meta:
        db_table = 'content"."film_work'
        verbose_name = _("Film")
        verbose_name_plural = _("Films")

    def __str__(self):
        return self.title


class GenreFilmWork(UUIDMixin):
    film_work = models.ForeignKey("FilmWork", on_delete=models.CASCADE)
    genre = models.ForeignKey("Genre", on_delete=models.CASCADE)
    created = models.DateTimeField(_("created"), auto_now_add=True)

    class Meta:
        db_table = 'content"."genre_film_work'


class Gender(models.TextChoices):
    MALE = "male", _("male")
    FEMALE = "female", _("female")


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(
        _("full_name"),
        max_length=MAX_LENGTH_NAME_FIELD)

    class Meta:
        db_table = 'content"."person'
        verbose_name = _("Person")
        verbose_name_plural = _("Persons")

    def __str__(self):
        return self.full_name


class RoleType(models.TextChoices):
    ACTOR = "actor", _("actor")
    DIRECTOR = "director", _("deirector")
    SCREEN_WRITER = "screenwriter", _("screenwriter")


class PersonFilmWork(UUIDMixin):
    film_work = models.ForeignKey("FilmWork", on_delete=models.CASCADE)
    person = models.ForeignKey("Person", on_delete=models.CASCADE)
    role = models.TextField(
        _("role"), choices=RoleType.choices, default=RoleType.ACTOR
    )
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content"."person_film_work'
