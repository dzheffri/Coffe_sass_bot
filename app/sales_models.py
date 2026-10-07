"""Sales request contracts; identity and audit fields are server-owned."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator


class SalesStatus(str, Enum):
    NEW = "NEW"
    NOT_CONTACTED = "NOT_CONTACTED"
    CONTACTED = "CONTACTED"
    REPLIED = "REPLIED"
    INTERESTED = "INTERESTED"
    DEMO = "DEMO"
    THINKING = "THINKING"
    CONNECTED = "CONNECTED"
    REJECTED = "REJECTED"
    DO_NOT_CONTACT = "DO_NOT_CONTACT"


class FollowupBucket(str, Enum):
    overdue = "overdue"
    today = "today"
    upcoming = "upcoming"


class SalesEventType(str, Enum):
    CONTACTED = "CONTACTED"
    NOTE_ADDED = "NOTE_ADDED"


class _SalesLeadFields(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: StrictStr | None = Field(default=None, min_length=1, max_length=300)
    city: StrictStr | None = Field(default=None, max_length=200)
    address: StrictStr | None = Field(default=None, max_length=1000)
    phone: StrictStr | None = Field(default=None, max_length=100)
    email: StrictStr | None = Field(default=None, max_length=320)
    instagram: StrictStr | None = Field(default=None, max_length=1000)
    telegram: StrictStr | None = Field(default=None, max_length=1000)
    website: StrictStr | None = Field(default=None, max_length=2000)
    google_maps_url: StrictStr | None = Field(default=None, max_length=2000)
    rating: float | None = Field(default=None, ge=0, le=5, allow_inf_nan=False)
    reviews_count: StrictInt | None = Field(default=None, ge=0, le=2147483647)
    status: SalesStatus | None = None
    score: float | None = Field(default=None, ge=0, le=100, allow_inf_nan=False)
    notes: StrictStr | None = Field(default=None, max_length=20000)
    next_followup_at: datetime | None = None
    connected_shop_id: StrictInt | None = Field(default=None, gt=0, le=9223372036854775807)

    @field_validator("name", "city", "address", "phone", "email", "instagram", "telegram",
                     "website", "google_maps_url", "notes", mode="before")
    @classmethod
    def clean_text(cls, value):
        if isinstance(value, str):
            return value.strip() or None
        return value

    @field_validator("rating", "score", mode="before")
    @classmethod
    def numeric_value(cls, value):
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))):
            raise ValueError("A number is required")
        return value

    @field_validator("next_followup_at")
    @classmethod
    def aware_followup(cls, value):
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("Follow-up must include a timezone offset")
        return value


class SalesLeadCreate(_SalesLeadFields):
    name: StrictStr = Field(min_length=1, max_length=300)
    status: SalesStatus = SalesStatus.NEW
    place_id: StrictStr | None = Field(default=None, max_length=500)
    source: StrictStr | None = Field(default=None, max_length=200)

    @field_validator("place_id", "source", mode="before")
    @classmethod
    def clean_source(cls, value):
        if isinstance(value, str):
            return value.strip() or None
        return value


class SalesLeadUpdate(_SalesLeadFields):
    @field_validator("name", "status")
    @classmethod
    def required_if_present(cls, value):
        if value is None:
            raise ValueError("This field cannot be null")
        return value


class SalesEventCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_type: SalesEventType
    note: StrictStr | None = Field(default=None, max_length=20000)
    metadata: dict[str, Any] | None = None

    @field_validator("note", mode="before")
    @classmethod
    def clean_note(cls, value):
        if isinstance(value, str):
            return value.strip() or None
        return value
