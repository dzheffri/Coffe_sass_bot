"""Provider abstraction for the Super Admin Sales CRM search.

The application never falls back to fabricated places.  Production uses
Google Places only when GOOGLE_PLACES_API_KEY is present; tests can inject the
small deterministic mock provider below without making network requests.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import httpx


class SalesSearchProviderError(Exception):
    def __init__(self, code: str, status_code: int, message: str):
        self.code = code
        self.status_code = status_code
        self.message = message
        super().__init__(message)


@dataclass(frozen=True)
class SalesSearchQuery:
    city: str
    radius_km: float
    category: str
    name: str | None
    limit: int


class SalesSearchProvider(Protocol):
    name: str

    def search(self, query: SalesSearchQuery) -> list[dict[str, Any]]:
        """Return normalized place dictionaries without writing to the CRM."""


def _text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = " ".join(value.split())
    return value or None


def _number(value: Any, *, integer: bool = False) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value) if integer else float(value)


class GooglePlacesProvider:
    """Google Places API (New) Text Search provider.

    Google's Text Search endpoint accepts a city in ``textQuery``.  A precise
    radius requires coordinates; callers can still narrow by city/category and
    the radius is kept in the normalized request for a future geocoded center.
    We do not silently claim that a radius was applied when no coordinates were
    supplied.
    """

    name = "google_places"
    endpoint = "https://places.googleapis.com/v1/places:searchText"
    field_mask = ",".join((
        "places.id",
        "places.displayName",
        "places.formattedAddress",
        "places.rating",
        "places.userRatingCount",
        "places.websiteUri",
        "places.nationalPhoneNumber",
        "places.googleMapsUri",
    ))

    def __init__(
        self,
        api_key: str,
        *,
        request: Callable[[str, Mapping[str, str], Mapping[str, Any]], Any] | None = None,
    ):
        self.api_key = api_key.strip()
        self._request = request or self._request_http

    def _request_http(self, url: str, headers: Mapping[str, str], payload: Mapping[str, Any]):
        try:
            with httpx.Client(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
                return client.post(url, headers=headers, json=payload)
        except httpx.TimeoutException as exc:
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_TIMEOUT", 503,
                "Пошук кавʼярень тимчасово недоступний.",
            ) from exc
        except httpx.HTTPError as exc:
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_UNAVAILABLE", 503,
                "Пошук кавʼярень тимчасово недоступний.",
            ) from exc

    @staticmethod
    def _place(place: Mapping[str, Any]) -> dict[str, Any] | None:
        place_id = _text(place.get("id"))
        display_name = place.get("displayName")
        if isinstance(display_name, Mapping):
            name = _text(display_name.get("text"))
        else:
            name = None
        if not place_id or not name:
            return None
        return {
            "place_id": place_id,
            "name": name,
            "address": _text(place.get("formattedAddress")),
            "rating": _number(place.get("rating")),
            "reviews_count": _number(place.get("userRatingCount"), integer=True),
            "website": _text(place.get("websiteUri")),
            "phone": _text(place.get("nationalPhoneNumber")),
            "google_maps_url": _text(place.get("googleMapsUri")),
            "source": "google_places",
        }

    def search(self, query: SalesSearchQuery) -> list[dict[str, Any]]:
        if not self.api_key:
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_NOT_CONFIGURED", 503,
                "Пошук Google Places не налаштовано на сервері.",
            )
        terms = [query.name, query.category, query.city]
        text_query = ", ".join(term for term in terms if term)
        payload: dict[str, Any] = {
            "textQuery": text_query,
            "includedType": "cafe",
            "pageSize": query.limit,
            "languageCode": "uk",
        }
        try:
            response = self._request(
                self.endpoint,
                {
                    "Content-Type": "application/json",
                    "X-Goog-Api-Key": self.api_key,
                    "X-Goog-FieldMask": self.field_mask,
                },
                payload,
            )
        except SalesSearchProviderError:
            raise
        except Exception as exc:
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_UNAVAILABLE", 503,
                "Пошук кавʼярень тимчасово недоступний.",
            ) from exc
        if response.status_code in (401, 403):
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_NOT_CONFIGURED", 503,
                "Пошук Google Places не налаштовано на сервері.",
            )
        if response.status_code >= 500:
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_UNAVAILABLE", 503,
                "Пошук кавʼярень тимчасово недоступний.",
            )
        if response.status_code >= 400:
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_REQUEST_INVALID", 502,
                "Не вдалося виконати пошук кавʼярень.",
            )
        try:
            body = response.json()
        except (TypeError, ValueError) as exc:
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_INVALID_RESPONSE", 502,
                "Сервіс пошуку повернув некоректну відповідь.",
            ) from exc
        places = body.get("places") if isinstance(body, Mapping) else None
        if not isinstance(places, list):
            return []
        return [normalized for place in places
                if isinstance(place, Mapping)
                for normalized in [self._place(place)] if normalized is not None]


class MockSalesSearchProvider:
    """Deterministic provider for unit tests; never selected by production."""

    name = "mock"

    def __init__(self, results: list[dict[str, Any]] | None = None):
        self.results = list(results or [])
        self.queries: list[SalesSearchQuery] = []

    def search(self, query: SalesSearchQuery) -> list[dict[str, Any]]:
        self.queries.append(query)
        return self.results[:query.limit]


def build_sales_search_provider() -> SalesSearchProvider:
    return GooglePlacesProvider(os.getenv("GOOGLE_PLACES_API_KEY", ""))
