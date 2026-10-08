"""Provider abstraction for the Super Admin Sales CRM search.

The application never falls back to fabricated places.  Production uses
Google Places only when GOOGLE_PLACES_API_KEY is present; tests can inject the
small deterministic mock provider below without making network requests.
"""

from __future__ import annotations

import os
import hashlib
import hmac
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx

from app.sales_search_usage import PostgresSalesSearchUsage


class SalesSearchProviderError(Exception):
    def __init__(self, code: str, status_code: int, message: str, usage=None):
        self.code = code
        self.status_code = status_code
        self.message = message
        self.usage = usage
        super().__init__(message)


@dataclass(frozen=True)
class SalesSearchQuery:
    city: str
    radius_km: float
    category: str
    name: str | None
    limit: int
    page_token: str | None = None


@dataclass(frozen=True)
class SalesSearchPage:
    items: list[dict[str, Any]]
    next_page_token: str | None = None
    usage: dict[str, Any] | None = None


class SalesSearchProvider(Protocol):
    name: str

    def search(self, query: SalesSearchQuery) -> list[dict[str, Any]]:
        """Return normalized place dictionaries without writing to the CRM."""


def _text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = " ".join(value.split())
    return value or None


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
        "places.addressComponents",
        "places.primaryType",
        "places.types",
        "places.websiteUri",
        "places.nationalPhoneNumber",
        "nextPageToken",
    ))

    def __init__(
        self,
        api_key: str,
        *,
        request: Callable[[str, Mapping[str, str], Mapping[str, Any]], Any] | None = None,
        usage: Any = None,
    ):
        self.api_key = api_key.strip()
        self._request = request or self._request_http
        self.usage = usage or PostgresSalesSearchUsage(None)

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
        # regionCode is a regional hint, not a country restriction. Fail closed
        # unless Google's structured response confirms both country and type.
        components = place.get("addressComponents")
        in_ukraine = isinstance(components, list) and any(
            isinstance(component, Mapping)
            and isinstance(component.get("types"), list)
            and "country" in component["types"]
            and component.get("shortText") == "UA"
            for component in components
        )
        types = place.get("types")
        coffee_shop = place.get("primaryType") == "coffee_shop" or (
            isinstance(types, list) and "coffee_shop" in types
        )
        if not in_ukraine or not coffee_shop:
            return None
        place_id = _text(place.get("id"))
        display_name = place.get("displayName")
        if isinstance(display_name, Mapping):
            name = _text(display_name.get("text"))
        else:
            name = None
        if not place_id or not name:
            return None
        instagram = _text(place.get("websiteUri"))
        try:
            parsed = urlsplit(instagram or "")
            host = (parsed.hostname or "").lower()
            if parsed.username or parsed.password or parsed.port or parsed.scheme not in {"http", "https"} or not (
                host in {"instagram.com", "www.instagram.com"}
            ):
                instagram = None
        except ValueError:
            instagram = None
        return {
            "place_id": place_id,
            "name": name,
            "address": _text(place.get("formattedAddress")),
            "instagram": instagram,
            "phone": _text(place.get("nationalPhoneNumber")),
            "source": "google_places",
        }

    def search(self, query: SalesSearchQuery) -> list[dict[str, Any]]:
        return self.search_page(query).items

    def _page_context(self, query: SalesSearchQuery):
        fingerprint = hashlib.sha256(json.dumps([
            query.city, query.radius_km, query.category, query.name, query.limit,
        ], ensure_ascii=False).encode()).hexdigest()
        if not query.page_token:
            return fingerprint, {"seen": [], "count": 0}
        try:
            data, signature = query.page_token.rsplit(".", 1)
            expected = hmac.new(self.api_key.encode(), data.encode(), hashlib.sha256).hexdigest()
            state = json.loads(data)
            if not hmac.compare_digest(signature, expected) or state["query"] != fingerprint:
                raise ValueError
            if not 0 < state["count"] < 60 or not isinstance(state["seen"], list):
                raise ValueError
            return fingerprint, state
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise SalesSearchProviderError("INVALID_SEARCH_PAGE_TOKEN", 422,
                                           "Почніть новий пошук кавʼярень.") from exc

    def search_page(self, query: SalesSearchQuery) -> SalesSearchPage:
        if not self.api_key:
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_NOT_CONFIGURED", 503,
                "Пошук Google Places не налаштовано на сервері.",
            )
        fingerprint, state = self._page_context(query)
        city = " ".join(query.city.split())
        if city.casefold() == "самар":
            city = "Самар, Дніпропетровська область"
        text_query = f"{city} Україна кав'ярня"
        if query.name:
            text_query += f" {query.name}"
        payload: dict[str, Any] = {
            "textQuery": text_query,
            "includedType": "coffee_shop",
            "strictTypeFiltering": True,
            "pageSize": query.limit,
            "languageCode": "uk",
            "regionCode": "UA",
        }
        if query.page_token:
            payload["pageToken"] = state["token"]
        # Each actual attempt (including a caller retry) reserves one request.
        # No transport retries or Google calls can bypass the shared PG guard.
        usage = self.usage.reserve()
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
        except SalesSearchProviderError as exc:
            exc.usage = usage
            raise
        except Exception as exc:
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_UNAVAILABLE", 503,
                "Пошук кавʼярень тимчасово недоступний.", usage,
            ) from exc
        if response.status_code in (401, 403):
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_NOT_CONFIGURED", 503,
                "Пошук Google Places не налаштовано на сервері.", usage,
            )
        if response.status_code >= 500:
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_UNAVAILABLE", 503,
                "Пошук кавʼярень тимчасово недоступний.", usage,
            )
        if response.status_code >= 400:
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_REQUEST_INVALID", 502,
                "Не вдалося виконати пошук кавʼярень.", usage,
            )
        try:
            body = response.json()
        except (TypeError, ValueError) as exc:
            raise SalesSearchProviderError(
                "SALES_SEARCH_PROVIDER_INVALID_RESPONSE", 502,
                "Сервіс пошуку повернув некоректну відповідь.", usage,
            ) from exc
        if not isinstance(body, Mapping):
            raise SalesSearchProviderError("SALES_SEARCH_PROVIDER_INVALID_RESPONSE", 502,
                                           "Сервіс пошуку повернув некоректну відповідь.", usage)
        places = body.get("places")
        if not isinstance(places, list):
            places = []
        places = places[:min(query.limit, 60 - state["count"])]
        seen = set(state["seen"])
        items = []
        for place in places:
            normalized = self._place(place) if isinstance(place, Mapping) else None
            if normalized and normalized["place_id"] not in seen:
                items.append(normalized)
                seen.add(normalized["place_id"])
        count = state["count"] + len(places)
        next_token = _text(body.get("nextPageToken"))
        if next_token and places and count < 60:
            data = json.dumps({"query": fingerprint, "seen": sorted(seen),
                               "count": count, "token": next_token}, separators=(",", ":"))
            next_token = data + "." + hmac.new(self.api_key.encode(), data.encode(), hashlib.sha256).hexdigest()
        else:
            next_token = None
        return SalesSearchPage(items, next_token, usage)


class MockSalesSearchProvider:
    """Deterministic provider for unit tests; never selected by production."""

    name = "mock"

    def __init__(self, results: list[dict[str, Any]] | None = None):
        self.results = list(results or [])
        self.queries: list[SalesSearchQuery] = []

    def search(self, query: SalesSearchQuery) -> list[dict[str, Any]]:
        self.queries.append(query)
        return self.results[:query.limit]


def build_sales_search_provider(connection_factory=None) -> SalesSearchProvider:
    return GooglePlacesProvider(
        os.getenv("GOOGLE_PLACES_API_KEY", ""),
        usage=PostgresSalesSearchUsage(connection_factory),
    )
