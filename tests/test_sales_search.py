"""Local-only Sales Search provider and router tests.

No Google request or production database connection is allowed in this module;
the router receives a deterministic provider and the Google adapter receives a
fake response.
"""

import pytest

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from app.api.sales import build_sales_router
from app.sales_search import (
    GooglePlacesProvider,
    MockSalesSearchProvider,
    SalesSearchQuery,
    SalesSearchProviderError,
    build_sales_search_provider,
)
from app.sales_search_usage import SalesSearchUsageError


class FakeUsage:
    def __init__(self, used=0, limit=950):
        self.used, self.limit = used, limit

    def snapshot(self):
        return {"used": self.used, "limit": self.limit, "month": "2026-10", "reset_at": "2026-11-01T00:00:00+02:00"}

    def reserve(self):
        if self.used >= self.limit:
            raise SalesSearchUsageError("MONTHLY_GOOGLE_PLACES_LIMIT_REACHED", 429,
                                        "Monthly cap", self.snapshot(), 60)
        self.used += 1
        return self.snapshot()


RESULTS = [{
    "place_id": "ChIJlocal1",
    "name": "Local Coffee",
    "address": "Київ, Хрещатик 1",
    "instagram": None,
    "phone": "+380441112233",
    "source": "mock",
}]


def _client(provider):
    def verify_actor(authorization, init_data):
        if authorization == "Bearer sales-superadmin":
            return {"user_id": 1, "telegram_id": 9001}
        raise HTTPException(401, detail={"code": "UNAUTHORIZED"})

    app = FastAPI()
    app.include_router(build_sales_router(
        verify_actor=verify_actor,
        superadmin_telegram_ids={9001},
        search_provider=provider,
    ))
    return TestClient(app)


def test_mock_provider_search_is_authenticated_and_normalized():
    provider = MockSalesSearchProvider(RESULTS)
    with _client(provider) as client:
        response = client.get(
            "/sales/search?city=%20%D0%9A%D0%B8%D1%97%D0%B2%20&radius_km=12&category=cafe&name=Local&limit=1",
            headers={"Authorization": "Bearer sales-superadmin"},
        )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["provider"] == "mock"
    assert body["items"] == RESULTS
    assert provider.queries == [SalesSearchQuery("Київ", 12, "cafe", "Local", 1)]


def test_search_requires_superadmin_and_rejects_unknown_query_fields():
    provider = MockSalesSearchProvider(RESULTS)
    with _client(provider) as client:
        assert client.get("/sales/search?city=Kyiv").status_code == 401
        response = client.get(
            "/sales/search?city=Kyiv&shop_id=1",
            headers={"Authorization": "Bearer sales-superadmin"},
        )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "UNEXPECTED_PARAMETER"


def test_google_provider_maps_places_without_network_or_exposing_key():
    calls = []

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"places": [{
                "id": "ChIJgoogle",
                "displayName": {"text": "Google Coffee"},
                "formattedAddress": "Kyiv, 1 Main St",
                "addressComponents": [{"shortText": "UA", "types": ["country"]}],
                "primaryType": "coffee_shop",
                "types": ["coffee_shop", "food"],
                "rating": 4.7,
                "userRatingCount": 88,
                "websiteUri": "https://google.example",
                "nationalPhoneNumber": "+380441234567",
                "googleMapsUri": "https://maps.google.com/?cid=2",
            }]}

    def fake_request(url, headers, payload):
        calls.append((url, headers, payload))
        return FakeResponse()

    provider = GooglePlacesProvider("local-test-key", request=fake_request, usage=FakeUsage())
    results = provider.search(SalesSearchQuery("Київ", 10, "cafe", None, 20))
    assert results[0]["place_id"] == "ChIJgoogle"
    assert results[0]["source"] == "google_places"
    assert results[0]["phone"] == "+380441234567"
    assert results[0]["instagram"] is None
    assert not {"rating", "reviews_count", "website", "google_maps_url"} & results[0].keys()
    assert calls[0][0].endswith("places:searchText")
    assert calls[0][1]["X-Goog-Api-Key"] == "local-test-key"
    assert calls[0][2] == {
        "textQuery": "Київ Україна кав'ярня", "includedType": "coffee_shop", "pageSize": 20,
        "strictTypeFiltering": True, "languageCode": "uk", "regionCode": "UA",
    }
    assert {"places.addressComponents", "places.primaryType", "places.types"} <= set(calls[0][1]["X-Goog-FieldMask"].split(","))
    assert set(calls[0][1]["X-Goog-FieldMask"].split(",")) == {
        "places.id", "places.displayName", "places.formattedAddress",
        "places.addressComponents", "places.primaryType", "places.types",
        "places.websiteUri", "places.nationalPhoneNumber", "nextPageToken",
    }


def _google_place(country="UA", primary_type="coffee_shop", types=None, **overrides):
    return {
        "id": "ChIJtest", "displayName": {"text": "Test Coffee"},
        "addressComponents": [{"shortText": country, "types": ["country"]}],
        "primaryType": primary_type,
        "types": [primary_type] if types is None else types,
        **overrides,
    }


@pytest.mark.parametrize("place,allowed", [
    (_google_place(), True),
    (_google_place(primary_type="cafe", types=["cafe", "coffee_shop"]), True),
    (_google_place(country="RU"), False),
    (_google_place(country="PL"), False),
    (_google_place(primary_type="restaurant"), False),
    (_google_place(primary_type="bar"), False),
    (_google_place(primary_type="bakery"), False),
    (_google_place(primary_type="cafe"), False),
    (_google_place(addressComponents=[]), False),
    (_google_place(addressComponents=[{"shortText": "UA", "types": ["locality"]}]), False),
    (_google_place(addressComponents=None), False),
    (_google_place(primary_type=None, types="coffee_shop"), False),
])
def test_google_results_require_structured_ukraine_and_coffee_shop(place, allowed):
    assert (GooglePlacesProvider._place(place) is not None) is allowed


def test_samar_search_qualifies_ukrainian_city_and_drops_russian_samara():
    calls = []

    class Response:
        status_code = 200

        def json(self):
            return {"places": [
                _google_place(id="ua-samar", formattedAddress="Самар, Дніпропетровська область, Україна"),
                _google_place(country="RU", id="ru-samara", formattedAddress="Самара, Россия"),
                _google_place(country="PL", id="pl-coffee"),
                _google_place(primary_type="restaurant", id="ua-restaurant"),
            ]}

    def request(url, headers, payload):
        calls.append(payload)
        return Response()

    provider = GooglePlacesProvider("fake-test-key", request=request, usage=FakeUsage())
    results = provider.search(SalesSearchQuery("Самар", 10, "restaurant", None, 20))
    assert [place["place_id"] for place in results] == ["ua-samar"]
    assert calls[0]["textQuery"] == "Самар, Дніпропетровська область Україна кав'ярня"
    assert calls[0]["includedType"] == "coffee_shop"
    assert calls[0]["strictTypeFiltering"] is True


def test_name_search_cannot_override_fixed_country_and_type():
    calls = []

    class Response:
        status_code = 200

        def json(self):
            return {"places": []}

    def request(url, headers, payload):
        calls.append(payload)
        return Response()

    provider = GooglePlacesProvider("fake-test-key", request=request, usage=FakeUsage())
    assert provider.search(SalesSearchQuery("Дніпро", 10, "bar", "Homie", 1)) == []
    assert calls[0]["textQuery"] == "Дніпро Україна кав'ярня Homie"
    assert calls[0]["regionCode"] == "UA"
    assert calls[0]["includedType"] == "coffee_shop"


def test_google_provider_without_key_fails_closed():
    provider = GooglePlacesProvider("")
    try:
        provider.search(SalesSearchQuery("Київ", 10, "cafe", None, 20))
    except SalesSearchProviderError as error:
        assert error.code == "SALES_SEARCH_PROVIDER_NOT_CONFIGURED"
        assert error.status_code == 503
    else:
        raise AssertionError("provider without GOOGLE_PLACES_API_KEY must not search")


def test_default_provider_is_google_adapter_and_never_a_mock():
    provider = build_sales_search_provider()
    assert provider.name == "google_places"


@pytest.mark.parametrize('url,expected', [
    ('https://instagram.com/coffee', 'https://instagram.com/coffee'),
    ('https://www.instagram.com/coffee?igsh=123', 'https://www.instagram.com/coffee?igsh=123'),
    ('https://coffee.example', None), ('https://facebook.com/coffee', None),
    ('https://instagram.com.evil.example', None), ('https://evilinstagram.com', None),
    ('javascript://instagram.com/x', None), ('https://business.instagram.com/x', None),
])
def test_instagram_uses_only_explicit_instagram_hosts(url, expected):
    place = GooglePlacesProvider._place(_google_place(websiteUri=url))
    assert place['instagram'] == expected
    assert 'website' not in place


def test_pagination_20_40_60_counts_every_http_attempt_and_caps_server_results():
    usage, calls = FakeUsage(), []

    class Response:
        status_code = 200
        def __init__(self, page):
            self.page = page
        def json(self):
            return {'places': [_google_place(id=f'place-{self.page * 20 + i}') for i in range(20)],
                    'nextPageToken': f'google-page-{self.page + 1}'}

    def request(url, headers, payload):
        calls.append(payload)
        return Response(len(calls) - 1)

    provider = GooglePlacesProvider('local-test-key', request=request, usage=usage)
    token, results = None, []
    for page_number in range(3):
        page = provider.search_page(SalesSearchQuery('Дніпро', 10, 'cafe', None, 20, token))
        results.extend(page.items)
        token = page.next_page_token
        assert len(results) == (page_number + 1) * 20
        assert page.usage['used'] == page_number + 1
    assert token is None
    assert calls[1]['pageToken'] == 'google-page-1'
    assert calls[2]['pageToken'] == 'google-page-2'
    assert usage.used == len(calls) == 3


def test_pagination_deduplicates_place_ids_and_rejects_forged_or_changed_query_before_google():
    usage, calls = FakeUsage(), []

    class Response:
        status_code = 200
        def json(self):
            return {'places': [_google_place(id='same'), _google_place(id=f'new-{len(calls)}')],
                    'nextPageToken': 'next'}

    def request(*args):
        calls.append(args)
        return Response()

    provider = GooglePlacesProvider('test-key', request=request, usage=usage)
    page1 = provider.search_page(SalesSearchQuery('Київ', 10, 'cafe', None, 20))
    page2 = provider.search_page(SalesSearchQuery('Київ', 10, 'cafe', None, 20, page1.next_page_token))
    assert [p['place_id'] for p in page2.items] == ['new-2']
    for query in [SalesSearchQuery('Львів', 10, 'cafe', None, 20, page1.next_page_token),
                  SalesSearchQuery('Київ', 10, 'cafe', None, 20, 'forged')]:
        with pytest.raises(SalesSearchProviderError, match='Почніть'):
            provider.search_page(query)
    assert usage.used == len(calls) == 2


def test_last_950_request_allowed_then_blocked_without_google_call():
    usage, calls = FakeUsage(949), []

    class Response:
        status_code = 200
        def json(self):
            return {'places': []}

    def request(*args):
        calls.append(args)
        return Response()

    provider = GooglePlacesProvider('test-key', request=request, usage=usage)
    with _client(provider) as client:
        assert client.get('/sales/search?city=Kyiv', headers={'Authorization': 'Bearer sales-superadmin'}).json()['usage']['used'] == 950
        response = client.get('/sales/search?city=Kyiv', headers={'Authorization': 'Bearer sales-superadmin'})
        assert response.status_code == 429
        assert response.json()['detail']['code'] == 'MONTHLY_GOOGLE_PLACES_LIMIT_REACHED'
        assert response.headers['retry-after'] == '60'
    assert len(calls) == 1


def test_google_failure_and_retry_each_reserve_once_and_unavailable_guard_never_calls_google():
    usage, calls = FakeUsage(), []
    def request(*args):
        calls.append(args)
        raise RuntimeError('fake transport error')
    provider = GooglePlacesProvider('test-key', request=request, usage=usage)
    for _ in range(2):
        with pytest.raises(SalesSearchProviderError) as error:
            provider.search(SalesSearchQuery('Київ', 10, 'cafe', None, 20))
        assert error.value.usage['used'] == len(calls)
    assert usage.used == 2
    provider = GooglePlacesProvider('test-key', request=request)
    with pytest.raises(SalesSearchUsageError):
        provider.search(SalesSearchQuery('Київ', 10, 'cafe', None, 20))
    assert len(calls) == 2


def test_usage_endpoint_requires_same_superadmin_guard_without_touching_google():
    with _client(MockSalesSearchProvider()) as client:
        assert client.get('/sales/search/usage').status_code == 401
        assert client.get('/sales/search/usage', headers={'Authorization': 'Bearer sales-superadmin'}).status_code == 503
