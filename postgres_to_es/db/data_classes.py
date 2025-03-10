import datetime as dt
from abc import ABC
from dataclasses import dataclass, field
from uuid import UUID


@dataclass
class TimeStampedMixin(ABC):
    created: dt.datetime
    modified: dt.datetime


@dataclass
class UUIDMixin(ABC):
    id: UUID

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)


@dataclass
class Filmwork(TimeStampedMixin, UUIDMixin):
    title: str
    description: str
    type: str
    creation_date: dt.datetime
    rating: float = field(default=0.0)


@dataclass
class Genre(TimeStampedMixin, UUIDMixin):
    name: str
    description: str


@dataclass
class Person(TimeStampedMixin, UUIDMixin):
    full_name: str


@dataclass
class GenreFilmwork(UUIDMixin):
    film_work_id: UUID
    genre_id: UUID
    created: dt.datetime = field(default_factory=dt.datetime.now)


@dataclass
class PersonFilmwork(UUIDMixin):
    film_work_id: UUID
    person_id: UUID
    role: str
    created: dt.datetime = field(default_factory=dt.datetime.now)
